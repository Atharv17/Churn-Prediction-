import java.time.{LocalDate}
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable

object WSDMChurnLabeller {
  def main(args: Array[String]) : Unit ={
    val spark = SparkSession
      .builder()
      .master("local")
      .getOrCreate()

    val data = spark
      .read
      .option("header", value = true)
      .csv("/Users/ardenchiu/wsdm_fix/wsdm_transactions_20170331.csv")

    val historyCutoff = "20170131"

    val historyData = data.filter(col("transaction_date")>="20170101" and col("transaction_date")<=lit(historyCutoff))
    val futureData = data.filter(col("transaction_date") > lit(historyCutoff))

    val calculateLastdayUDF = udf(calculateLastday _)
    val userExpire = historyData
      .groupBy("msno")
      .agg(
        calculateLastdayUDF(
          collect_list(
            struct(
              col("payment_method_id"),
              col("payment_plan_days"),
              col("plan_list_price"),
              col("transaction_date"),
              col("membership_expire_date"),
              col("is_cancel")
            )
          )
        ).alias("last_expire")
      )

    val predictionCandidates = userExpire
      .filter(
        col("last_expire") >= "20170201" and col("last_expire") <= "20170228"
      )
      .select("msno", "last_expire")


    val joinedData = predictionCandidates
      .join(futureData,Seq("msno"), "left_outer")

    val noActivity = joinedData
      .filter(col("payment_method_id").isNull)
      .withColumn("is_churn", lit(true))


    val calculateRenewalGapUDF = udf(calculateRenewalGap _)
    val renewals = joinedData
      .filter(col("payment_method_id").isNotNull)
      .groupBy("msno", "last_expire")
      .agg(
        calculateRenewalGapUDF(
          collect_list(
            struct(
              col("payment_method_id"),
              col("payment_plan_days"),
              col("plan_list_price"),
              col("transaction_date"),
              col("membership_expire_date"),
              col("is_cancel")
            )
          ),
          col("last_expire")
        ).alias("gap")
      )

    val validRenewals = renewals.filter(col("gap") < 30)
      .withColumn("is_churn", lit(false))
    val lateRenewals = renewals.filter(col("gap") >= 30)
      .withColumn("is_churn", lit(true))

    val resultSet = validRenewals
      .select("msno","is_churn")
      .union(
        lateRenewals
          .select("msno","is_churn")
          .union(
            noActivity.select("msno","is_churn")
          )
      )

    resultSet.repartition(1).write.option("header",value=true).csv("sample_user_label")
    spark.stop()

  }


  def calculateLastday(wrappedArray: mutable.WrappedArray[Row]) :String ={
    val orderedList = wrappedArray.sortWith((x:Row, y:Row) => {
      if(x.getAs[String]("transaction_date") != y.getAs[String]("transaction_date")) {
        x.getAs[String]("transaction_date") < y.getAs[String]("transaction_date")
      } else {
        val x_sig = x.getAs[String]("plan_list_price") +
          x.getAs[String]("payment_plan_days") +
          x.getAs[String]("payment_method_id")


        val y_sig = y.getAs[String]("plan_list_price") +
          y.getAs[String]("payment_plan_days") +
          y.getAs[String]("payment_method_id")

        //same plan, always subscribe then unsubscribe
        if(x_sig != y_sig) {
          x_sig > y_sig
        } else {
          if(x.getAs[String]("is_cancel")== "1" && y.getAs[String]("is_cancel") == "1") {
            //multiple cancel, consecutive cancels should only put the expiration date earlier
            x.getAs[String]("membership_expire_date") > y.getAs[String]("membership_expire_date")
          } else if(x.getAs[String]("is_cancel")== "0" && y.getAs[String]("is_cancel") == "0") {
            //multiple renewal, expiration date keeps extending
            x.getAs[String]("membership_expire_date") < y.getAs[String]("membership_expire_date")
          } else {
            //same day same plan transaction: subscription preceeds cancellation
            x.getAs[String]("is_cancel") < y.getAs[String]("is_cancel")
          }
        }
      }
    })
    orderedList.last.getAs[String]("membership_expire_date")
  }

  def calculateRenewalGap(log:mutable.WrappedArray[Row], lastExpiration: String): Int = {
    val orderedDates = log.sortWith((x:Row, y:Row) => {
      if(x.getAs[String]("transaction_date") != y.getAs[String]("transaction_date")) {
        x.getAs[String]("transaction_date") < y.getAs[String]("transaction_date")
      } else {
        val x_sig = x.getAs[String]("plan_list_price") +
          x.getAs[String]("payment_plan_days") +
          x.getAs[String]("payment_method_id")


        val y_sig = y.getAs[String]("plan_list_price") +
          y.getAs[String]("payment_plan_days") +
          y.getAs[String]("payment_method_id")

        //same data same plan transaction, assumption: subscribe then unsubscribe
        if(x_sig != y_sig) {
          x_sig > y_sig
        } else {
          if(x.getAs[String]("is_cancel")== "1" && y.getAs[String]("is_cancel") == "1") {
            //multiple cancel of same plan, consecutive cancels should only put the expiration date earlier
            x.getAs[String]("membership_expire_date") > y.getAs[String]("membership_expire_date")
          } else if(x.getAs[String]("is_cancel")== "0" && y.getAs[String]("is_cancel") == "0") {
            //multiple renewal, expire date keep extending
            x.getAs[String]("membership_expire_date") < y.getAs[String]("membership_expire_date")
          } else {
            //same date cancel should follow subscription
            x.getAs[String]("is_cancel") < y.getAs[String]("is_cancel")
          }
        }
      }
    })
	
	//Search for the first subscription after expiration
	//If active cancel is the first action, find the gap between the cancellation and renewal
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    var lastExpireDate = LocalDate.parse(s"${lastExpiration.substring(0,4)}-${lastExpiration.substring(4,6)}-${lastExpiration.substring(6,8)}", formatter)
    var gap = 9999
    for(
      date <- orderedDates
      if gap == 9999
    ) {
      val transString = date.getAs[String]("transaction_date")
      val transDate = LocalDate.parse(s"${transString.substring(0,4)}-${transString.substring(4,6)}-${transString.substring(6,8)}", formatter)
      val expireString = date.getAs[String]("membership_expire_date")
      val expireDate = LocalDate.parse(s"${expireString.substring(0,4)}-${expireString.substring(4,6)}-${expireString.substring(6,8)}", formatter)
      val isCancel = date.getAs[String]("is_cancel")

      if(isCancel == "1") {
        if(expireDate.isBefore(lastExpireDate)) {
          lastExpireDate = expireDate
        }
      } else {
        gap = ChronoUnit.DAYS.between(lastExpireDate, transDate).toInt
      }
    }
    gap
  }
}

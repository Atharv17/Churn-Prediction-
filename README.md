# Churn Prediction of Customer 

Nowadays almost every streaming or service providing / companieshas weekly/monthly/yearly subscription plans , So it is very necessaryto retain a customer. If there are high chances that a customer is goingto leave the subscription  then the company can come with somestrategy to avoid it .This can be predicted with the help of churn prediction , and here wehave a churn prediction problem problem of a music streamingcompany called kkbox where the aim is  to predict whether the customer will churn the subscription after one months or not , orbasically the customer will renew subscription in 30 days or not .Some of the important features to determine churn rate aretransaction date , membership expiration rate and is_cancel , whereis_cancel is our target variable .Date is a time series data , divided over the period of months ,Train data is the data of the customer whose subscription is expiring inmonth of february,2017  and test data dataset contains the data hosesubscription expires in month of march,2017 so we are to see churnrenewal for train data in month of  march,2017 and churn renewal fortest data in month of april ,2017.User logs , information of members and transaction details are given inthe dataWe have to predict whether the customer will churn or not (0 or 1) .Once the company knows the customer is going to churn it can beavoided by some marketing techniques etc.


### Source  Links
* https://www.kaggle.com/c/kkbox-churn-prediction-challenge/overview
* https://www.kaggle.com/c/kkbox-churn-prediction-challenge/data

### Data overview
1. Member_v3 - Information about the customers (customer_id,gender,Payment method,Payment date)
1. Information about behaviour of different user (number of uniquesongs , different percentiles of how many songs played , total songs played)
1. Transaction  - Information regarding the each transaction and its details(payment_method,plan_price , validity , is_autorenew, membership expirationdate , is_cancel)
1. Train - (customer_id,is_churn)
1. Sample_Submission_zero - The predicted Values that it will churn or not 

Rest 4 files are User_logs,transaction, train, sample_submission but it's the data for the predicted renewal month 


### Useful Links and Blogs 
* https://arxiv.org/abs/1802.03396
* https://www.profitwell.com/blog/churn-prediction
* https://www.kaggle.com/jeru666/did-you-think-of-these-features
* https://towardsdatascience.com/machine-learning-powered-churn-analysis-for-modern-day-business-leaders-ad2177e1cb0d


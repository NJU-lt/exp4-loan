from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
sc = SparkContext.getOrCreate()
def f(x):
    rel = {}
    rel['loan_id']=x[0]
    rel['user_id'] = x[1]
    rel['employment_type']=x[9]
    rel['year_of_loan']=x[3]
    rel['monthly_payment']=x[5]
    rel['total_loan']=x[2]
    rel['work_year']=x[11]
    rel['censor_status']=x[14]
    return rel
loanDF = sc.textFile('train_data.csv').map(lambda line:line.split(',')).map(lambda x:Row(**f(x))).toDF()
loanDF.createOrReplaceTempView("loan")


# sql1 = "SELECT employment_type,count(loan_id) From loan GROUP BY employment_type"
# loanDF = spark.sql(sql1)
# loanDF.rdd.map(lambda t:"employment_type:" + str(t[0])+","+"count:"+str(t[1])).foreach(print)

sql2 = "SELECT user_id,year_of_loan*monthly_payment*12 - total_loan as total_money FROM loan "
loanDF = spark.sql(sql2)
loanDF.select("user_id","total_money").write.format("csv").save("3_2.csv")


sc.stop()

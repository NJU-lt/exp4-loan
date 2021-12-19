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
    workyear = 0
    if x[11]=="":
        workyear = 0
    else:
        year = x[11].split(" ",1)
        if year[0] == "10+":
            workyear = 10
        elif year[0][0] == "<":
            workyear = 0
        else:
            workyear = int(year[0])
    rel['work_year']=workyear
    rel['censor_status']=x[14]
    return rel
loanDF = sc.textFile('train_data.csv').map(lambda line:line.split(',')).map(lambda x:Row(**f(x))).toDF()
loanDF.createOrReplaceTempView("loan")


sql3 = "SELECT user_id,censor_status,work_year FROM loan Where work_year>5"
loanDF = spark.sql(sql3)
loanDF.select("user_id","censor_status","work_year").write.format("csv").save("3_3.csv")
sc.stop()
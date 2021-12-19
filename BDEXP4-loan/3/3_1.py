from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import pyspark.sql.functions as f
sc = SparkContext.getOrCreate()
def f(x):
    rel = {}
    rel['loan_id']=x[0]
    rel['employment_type']=x[9]
    return rel
loanDF = sc.textFile('train_data.csv').map(lambda line:line.split(',')).map(lambda x:Row(**f(x))).toDF()
loanDF.createOrReplaceTempView("loan")

all_count = loanDF.count()
sql = "SELECT employment_type,count(loan_id)/(select count(loan_id) as all_count from loan ) as count1 From loan GROUP BY employment_type"
loanDF = spark.sql(sql)

loanDF.select("employment_type","count1").write.format("csv").save("3_1.csv")

sc.stop()


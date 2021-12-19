from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from pyspark import SparkContext
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import OneHotEncoder
from pyspark.sql.functions import *
from pyspark.ml.feature import MaxAbsScaler,StandardScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.classification import LinearSVC
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml import Pipeline,PipelineModel

from pyspark.ml.linalg import Vectors
import numpy as np
import sys
import pyspark.sql.types as T
from datetime import *
from pyecharts.charts import Pie
from pyecharts.charts import Bar
from pyecharts import options as opts
from pyecharts.charts import Page

@f.udf(returnType = IntegerType())
def work_year(x):
    workyear = 0
    if x:
        year = str(x).split(" ",1)
        if year[0] == "10+":
            workyear = 10
        elif year[0][0] == "<":
            workyear = 0
        else:
            workyear = int(year[0])
    return workyear

spark=SparkSession.builder.appName("4").getOrCreate()
df_train = spark.read.csv("test/train_data.csv",header='true',inferSchema='true')
df_train.createOrReplaceTempView("loan")

#work_year
df_train= df_train.withColumn("work_year",work_year(f.col("work_year")))

#
df_train = df_train.na.fill(-1)
df_train = df_train.na.fill("-1")

# work_type
work_type_stringIndexer = StringIndexer(inputCol="work_type",outputCol="work_type_class",stringOrderType="frequencyDesc")
df_train = work_type_stringIndexer.fit(df_train).transform(df_train)
work_type_encoder = OneHotEncoder(inputCol="work_type_class",outputCol="work_type_onehot").setDropLast(False)
df_train = work_type_encoder.fit(df_train).transform(df_train)

#employer_type
employer_type_stringIndexer = StringIndexer(inputCol="employer_type",outputCol="employer_type_class",stringOrderType="frequencyDesc")
df_train = employer_type_stringIndexer.fit(df_train).transform(df_train)
employer_type_encoder = OneHotEncoder(inputCol="employer_type_class",outputCol="employer_type_onehot").setDropLast(False)
df_train = employer_type_encoder.fit(df_train).transform(df_train)

# industry
industry_stringIndexer = StringIndexer(inputCol="industry",outputCol="industry_class",stringOrderType="frequencyDesc")
df_train = industry_stringIndexer.fit(df_train).transform(df_train)
industry_encoder = OneHotEncoder(inputCol="industry_class",outputCol="industry_onehot").setDropLast(False)
df_train = industry_encoder.fit(df_train).transform(df_train)

#issue_data
@f.udf(returnType = IntegerType())
def issuedata(x):
    time = 0
    if x == "-1":
        time = 0
    else:
        timeString = x.split("-")
        year = int(timeString[0])
        month = int(timeString[1])
        day = int(timeString[2])
        time1 = datetime(2007, 7, 1)
        time2 = datetime(year, month, day)
        time = (time2-time1).days//30
    return time
df_train= df_train.withColumn("issue_date",issuedata(f.col("issue_date")))

total_y = []
attr = []
for i in range(7):
    attr.append(chr(ord('A')+i))
    total_y.append(df_train.filter(df_train['class'] == attr[i]).count())



pie = (
    Pie()
        .add("网络贷款等级", [list(z) for z in zip(attr, total_y)])
        .set_global_opts(title_opts=opts.TitleOpts(title="网络贷款等级分布"))
        .set_series_opts(
        tooltip_opts=opts.TooltipOpts(trigger="item", formatter="{a} <br/>{b}: {c} ({d}%)"),
        label_opts=opts.LabelOpts(formatter="{b}: {c} ({d}%)")
    )
)

attr = ["0-2000", "2000-4000", "4000-6000","6000-8000","8000-10000",
        "10000-12000","12000-14000","14000-16000","16000-18000","18000-20000",
        "20000-22000","22000-24000","24000-26000","26000-28000","28000-30000",
        "30000-32000","32000-34000","34000-36000","36000-38000","38000-40000"]
money_list = []

for i in range(20):
    money_list.append(df_train.filter((df_train['total_loan'] < (i+1)*2000) & (df_train['total_loan'] >= i*2000)).count())
bar = (
    Bar()
        .add_xaxis(attr)
        .add_yaxis("频率", money_list)
        .set_global_opts(title_opts=opts.TitleOpts(title="网络贷款金额分布"))
)


page = Page()
page.add(pie)
page.add(bar)

page.render('age_OverDue.html')

# 






# #class
class_stringIndexer = StringIndexer(inputCol="class",outputCol="class_class")
df_train = class_stringIndexer.fit(df_train).transform(df_train)

#sub_class
subclass_stringIndexer = StringIndexer(inputCol="sub_class",outputCol="subclass_class")
df_train = subclass_stringIndexer.fit(df_train).transform(df_train)


def f(loan):
    print(loan.work_type)
result.foreach(f)
df_train.select("work_type_onehot","employer_type_onehot","industry_onehot","issue_date","class_class","subclass_class").show(10)
df_train.printSchema()
# 3. 划分数据集

featuresArray = ['total_loan', 'year_of_loan', 'interest', 'monthly_payment',\
                 'class_class','subclass_class','work_type_onehot','work_year',\
                 'employer_type_onehot','industry_onehot','issue_date',\
                 'house_exist','house_loan_status',\
                 'censor_status','marriage','offsprings','use','post_code',\
                 'region','debt_loan_ratio','del_in_18month','scoring_low',\
                 'scoring_high','pub_dero_bankrup','early_return','early_return_amount',\
                 'early_return_amount_3mon','recircle_b','recircle_u','initial_list_status',\
                 'title','policy_code','f0','f1','f2','f3','f4','f5']
assembler = VectorAssembler().setInputCols(featuresArray).setOutputCol("features")
scaler = StandardScaler(inputCol="features", outputCol="features_scaled", withMean=True, withStd=True)

df_train= assembler.transform(df_train)
df_train = scaler.fit(df_train).transform(df_train)
df_train.select("features_scaled").show(10)

trainingData, testData = df_train.randomSplit([0.8,0.2])
def eva_index(Predictions):
    # 使用混淆矩阵评估模型性能[[TP,FN],[TN,FP]]
    print("-------------------------------------------------------------------")
    TP = Predictions.filter(Predictions['prediction'] == 1).filter(Predictions['is_default'] == 1).count()
    FN = Predictions.filter(Predictions['prediction'] == 0).filter(Predictions['is_default'] == 1).count()
    TN = Predictions.filter(Predictions['prediction'] == 0).filter(Predictions['is_default'] == 0).count()
    FP = Predictions.filter(Predictions['prediction'] == 1).filter(Predictions['is_default'] == 0).count()

    # 计算查准率 TP/（TP+FP）
    precision = TP/(TP+FP)
    # 计算查全率 TP/（TP+FN）
    recall = TP/(TP+FN)
    # 计算F1值 （TP+TN)/(TP+TN+FP+FN)
    F1 =(2 * precision * recall)/(precision + recall)
    acc = (TP + TN) / (TP + FN + TN + FP)
    print("The 查准率 is :",precision)
    print("The 查全率 is :",recall)
    print('The F1 is :',F1)
    print('The 准确率 s :', acc)
    # AUC为roc曲线下的面积，AUC越接近与1.0说明检测方法的真实性越高
    auc = BinaryClassificationEvaluator(labelCol="is_default").evaluate(Predictions)
    print("The auc分数 is :",auc)


from pyspark.sql.functions import when
trainingData = trainingData.withColumn("classWeights",when(trainingData.is_default == 1,0.7).otherwise(0.3))

lr = LogisticRegression(labelCol="is_default",featuresCol="features_scaled",weightCol="classWeights").setMaxIter(10).setRegParam(0.01).\
    setElasticNetParam(0.8).fit(trainingData)
lrPredictions = lr.transform(testData)
eva_index(lrPredictions)

svm = LinearSVC(maxIter=100,labelCol="is_default",featuresCol="features_scaled",weightCol="classWeights").fit(trainingData)
svmPredictions = svm.transform(testData)
eva_index(svmPredictions)

dt = DecisionTreeClassifier(labelCol="is_default",featuresCol="features_scaled",weightCol="classWeights").fit(trainingData)
dtPredictions = dt.transform(testData)
eva_index(dtPredictions)

rf = RandomForestClassifier(labelCol="is_default",featuresCol="features_scaled",weightCol="classWeights",maxBins=700,numTrees=50).fit(trainingData)
rfPredictions = rf.transform(testData)
eva_index(rfPredictions)



spark.stop()



from pyspark import SparkContext
def map_func(x):
    s = x.split(",")
    total_loan = round(float(s[2]))
    intervalmin = (total_loan // 1000)*1000
    intervalmax = intervalmin + 1000
    interval = "("+str(intervalmin)+","+str(intervalmax)+")"
    return (interval,1)
sc = SparkContext()
lines = sc.textFile("train_data.csv").map(lambda x:map_func(x)).cache()
result = lines.reduceByKey(lambda x,y:x+y).collect()
with open("2.csv","w") as file:
    for i in result:
        file.write("%s%s,%d%s\n" % ("(",i[0],i[1],")"))
file.close()
print(result)
sc.stop()
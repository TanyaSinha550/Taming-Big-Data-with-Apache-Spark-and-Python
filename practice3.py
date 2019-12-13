from pyspark import SparkConf,SparkContext

conf=SparkConf().setMaster("local").setAppName("popular")
sc=SparkContext(conf=conf)

lines=sc.textFile("file:///SparkCourse/ml-100k/u.data")
rdd1=lines.map(lambda x:(int(x.split()[1]),1))
count=rdd1.reduceByKey(lambda x,y:x+y)
flip=count.map(lambda x:(x[1],x[0]))
sorted=flip.sortByKey()
result=sorted.collect()

for res in result:
    print(res)


from pyspark import SparkConf,SparkContext

conf=SparkConf().setMaster("local").setAppName("MaxTemp")
sc=SparkContext(conf=conf)

def Parsed(line):
    fields=line.split(',')
    stationId=fields[0]
    entryType=fields[2]
    Temp=float(fields[3])*0.1*(9.0/5.0)+32.0
    return(stationId,entryType,Temp)

lines=sc.textFile("file:///SparkCourse/1800.csv")
parsedLines=lines.map(Parsed)

tempMax=parsedLines.filter(lambda x:"TMAX" in x[1])
station=tempMax.map(lambda x:(x[0],x[2]))
result=station.reduceByKey(lambda x,y : max(x,y))
results=result.collect()
for res in results:
    print(res[0] + "\t{:.2f}F".format(res[1]))
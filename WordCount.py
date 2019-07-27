from pyspark import SparkConf, SparkContext


conf = SparkConf().setMaster("yarn-cluster").setAppName("MyApp")

sc = SparkContext(conf=conf)

# logFile = "file:///D:/CrackCaptcha.log"
logFile = "/mytest/testspark/access.log"
rdd = sc.textFile(logFile, 2).cache()
num1 = rdd.filter(lambda l: '.jpg' in l).count()
num2 = rdd.filter(lambda l: '.html' in l).count()

print("Lines with a:%s,Lines with b:%s" % (num1,num2))

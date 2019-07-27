from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext

conf = SparkConf()
conf.setAppName('TestDstream')
conf.setMaster('yarn-cluster')
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 3)

lines = ssc.textFileStream('File:///develop/testdata/')
words = lines.flatMap(lambda x: x.split(' '))
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda a, b: a+b)

wordCounts.pprint()
ssc.start()
ssc.awaitTermination()

# 创建StreamingContext对象
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
conf = SparkConf()
conf.setAppName('TestDstream')
conf.setMaster('local[2]')
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 10)  # 每隔10秒去启动一次流计算
lines = ssc.textFileStream('file:///C:/Users/Lenovo/Desktop/streaming.txt')
words = lines.flatMap(lambda line: line.split(' '))
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda a, b: a+b)
wordCounts.pprint()
ssc.start()
ssc.awaitTermination()
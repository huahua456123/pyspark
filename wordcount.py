from pyspark import SparkContext, SparkConf
conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf=conf)
logFile = 'file:///C:/Users/Lenovo/Desktop/test.txt'
logdata = sc.textFile(logFile, 2).cache()
g = logdata.map(lambda x: x.split(',')).flatMap(lambda x: x).flatMap(lambda x: x.split(' ')).cache()
print(g.collect())
print(logdata.collect())
numAs = logdata.filter(lambda line: 'a' in line).count()  # 统计单词a总共出现次数
numbs = logdata.filter(lambda line: 'love' in line).count()


"""
groupByKey也是对每个key进行操作，但只生成一个sequence，groupByKey本身不能自定义函数，需要先用groupByKey生成
RDD,然后才能对此RDD通过map进行自定义函数操作
reduceByKey用于对每个key对应的多个value进行merge操作，最重要的是它能够在本地先进行merge操作，并且merge操作可以通过函数自定义
"""
numsa = g.filter(lambda line: 'a' in line).count()
print('lines with a: %d,lines with b: %d, lines with b: %d' % (numAs, numbs, numsa))
reducebykey = g.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x+y)
print(reducebykey .collect())
groupbykey = g.map(lambda x: (x, 1)).groupByKey().cache()
print(groupbykey.collect())
groupbykey1 = groupbykey.map(lambda x: (x[0], sum(x[1])))
print(groupbykey1.collect())

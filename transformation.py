from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName('my app').setMaster('local')
sc = SparkContext(conf=conf)
rdd = sc.parallelize([('spark', 2), ('hadoop', 6), ('hadoop', 4), ('spark', 6)])
# mapValues key不变，对值进行操作,这里相当于将值变为('spark', (2,1))……
rdd1 = rdd.mapValues(lambda x: (x, 1)).\
    reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1])).\
    mapValues(lambda x: x[0]/x[1]).collect()
print(rdd1)

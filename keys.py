from pyspark import SparkContext, SparkConf
conf = SparkConf().setMaster("local").setAppName("my app")
sc = SparkContext(conf=conf)
list1 = [('hadoop', 1), ('spark', 1), ('hive', 1), ('spark', 1)]
pariRDD = sc.parallelize(list1)
pariRDD.keys().foreach(print)
pariRDD.values().foreach(print)
# sortByKey
pariRDD.sortByKey(False).foreach(print)  # 默认是升序
# sortBy
d1 = sc.parallelize([('c', 8), ('b', 25), ('c', 17), ('a', 42), ('b', 4), ('d', 9)])
d2 = d1.reduceByKey(lambda a, b: a+b).sortByKey(False).collect()
print(d2)
d3 = d1.reduceByKey(lambda a, b: a+b).sortBy(lambda a: a[1], False).cache()
print(d3.collect())

#  mapValues
pariRDD1 = pariRDD.mapValues(lambda x: x+1)
pariRDD1.foreach(print)

# join
"""
join就表示内连接，对于内连接，对于给定的两个输入数据集(K,V1)和(K,V2),只有在两个数据
集中都存在的key才会被输出，最终得到一个(K, (V1,V2))类型的数据集
"""
pairRDD4 = sc.parallelize([('spark', 1), ('spark2', 2), ('hadoop', 3), ('hadoop', 5)])
pairRDD2 = sc.parallelize([('spark', 'fast')])
pairRDD3 = pairRDD4.join(pairRDD2)
pairRDD3.foreach(print)



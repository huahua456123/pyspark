# from datetime import datetime
# start = datetime.now()
# print(start)
# print('hello world ')
# for i in range(10):
#     print("hu ming fa must be find a girlfriends")

from pyspark import SparkContext

sc = SparkContext('local')
# 第二个参数2代表的是分区数，默认为1
old = sc.parallelize([1, 2, 3, 4, 5], 2)
newMap = old.map(lambda x: (x, x ** 2))
newReduce = old.reduce(lambda a, b: a + b)
print(newMap.glom().collect())

# # 缓存
from pyspark import SparkContext, SparkConf
# sc = SparkContext('local')
# list1 = ['hadoop', 'spark', 'hive']
# rdd = sc.parallelize(list1)
# rdd.cache()  # 会调用persist(MEMORY_ONLY)，但是，语句执行到这里，并不会缓存rdd,因为这时rdd还没有被计算生成
# print(rdd.count())  # 第一次行动操作，触发一次真正从头到尾的计算，这时上面的rdd.cache()才会被执行，把这个rdd放到缓存中
# print(','.join(rdd.collect()))  # 第二次行动操作，不需要触发从头到尾的计算，只需要重复使用上面缓存中的rdd
#
# # 分区
# """
# 1、增加并行度
# 2、减少通信开销
# """
# list2 = [1, 2, 3, 4, 5]
# rdd = sc.parallelize(list2, 2)  # 设置两个分区
# # 使用reparitition方法重新设置分区个数
# data = sc.parallelize([1, 2, 3, 4, 5])
# print(len(data.glom().collect()))  # 显示data这个rdd的分区数量
# rdd = data.repartition(1)  # 对data这个rdd重新进行分区
# print(len(rdd.glom().collect()))

# 自定义分区方法


def MyPartitioner(key):
    print('MyPartitioner is funning')
    print('the key is %d' % key)
    return key % 10


def main():
    print('the main function is running')
    conf = SparkConf().setMaster('local').setAppName('MyApp')
    sc = SparkContext(conf=conf)
    data = sc.parallelize(range(10), 5)
    data.map(lambda x: (x, 1))\
        .partitionBy(10, MyPartitioner)\
        .map(lambda x: x[0])\
        .saveAsTextFile('file:///E:/java1')  # 会生成10个文件，所以这里是目录,注意这里的目录必须不存在


if __name__ == '__main__':
    main()



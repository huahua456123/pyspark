from pyspark import SparkContext, SparkConf
conf = SparkConf().setMaster('local').setAppName('my app')
sc = SparkContext(conf=conf)
# 把rdd写入到文本文件中
textFile = sc.textFile("file:///C:/Users/Lenovo/Desktop/test.txt")  # 注意这里是三个///
textFile.saveAsTextFile("file://C:/Users/Lenovo/Desktop/write")  # 注意这里是目录
# 再次把数据加载在RDD中
textFile = sc.\
    textFile("file://C:/Users/Lenovo/Desktop/write")  # 把目录名称就可把目录下的所有文件加载进来

# 分布式文件系统HDFS的数据读写
textFile = sc.textFile("hdfs://localhost:90000/user/hadoop/word.txt")  # 这里是两个//
textFile.first()
# 三条语句都是等价的
# textFile = sc.textFile("/user/hadoop/word.txt")
# textFile = sc.textFile("word.txt")
# textFile.first()
#

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
# 在交互式环境启动pysaprk以后，pyspark就默认提供了一个SparkContext对象(名称为sc)和一个
# SparkSession对象(名称为spark)
spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
df = spark.read.json("file:///D:/spark/resources/people.json")
df.show()
print(type(df))

peopelDF = spark.read.format('json').load("file:///D:/spark/resources/people.json")
peopelDF.select('name', 'age').write.format('json').save('file:///D:/spark/newpeople.json')  # newpeople.json是目录不是文件

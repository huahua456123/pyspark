from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import *
spark = spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()

# 下面生成表头
schemaString = 'name age'
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(' ')]
schema = StructType(fields)
# 下面生成表中的记录
lines = spark.sparkContext.textFile('file:///D:/spark/resources/people.txt')
parts = lines.map(lambda x: x.split(','))
people = parts.map(lambda p: Row(p[0], p[1].strip()))
# 下面把表头和表中的记录拼接在一起
schemaPeople = spark.createDataFrame(people, schema)  # 表中记录放前面，表头放后面
# 注册一个临时表供下面查询使用
schemaPeople.createOrReplaceTempView('people')
results = spark.sql('select name, age from people')
results.show()


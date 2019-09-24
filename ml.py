from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer

spark = SparkSession.builder.master('local').appName('word count').getOrCreate()
# Prepare training documents from a list of (id, text, label)tuples
training = spark.createDataFrame([
    (0, 'a b c d e spark', 1.0),
    (1, 'b d', 0.0),
    (2, 'spark f g h', 1.0),
    (3, 'hadoop mapreduce', 0.0)
    ], ['id', 'text', 'label'])
tokenizer = Tokenizer(inputCol='text', outputCol='words')
hashingTf = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol='features')
lr = LogisticRegression(maxIter=10, regParam=0.001)
#  按照处理逻辑有序地组织PipelineStages,创建Pipeline
pipeline = Pipeline(stages=[tokenizer, hashingTf, lr])
# 现在构建的Pipeline本质上是一个Estimator,在它的fit()方法运行之后，它将产生一个PipelineModel,它是一个transformer
model = pipeline.fit(training)
# 可以看到，model的类型是一个PipelineModel，这个流水线模型将在数据测试的时候使用


# 构建测试数据
test = spark.createDataFrame([
    (4, 'spark i j k'),
    (5, 'l m n'),
    (6, 'spark hadoop spark'),
    (7, 'apache hadoop')
], ['id', 'text'])


# 生成预测结果
prediction = model.transform(test)
selected = prediction.select('id', 'text', 'probability', 'prediction')
for row in selected.collect():
    rid, text, prob, prediction = row
    print("(%d, %s)-->prob=%s, prediction=%f"%(rid, text, str(prob), prediction))

from pyspark.ml.linalg import Vector, Vectors
from pyspark.sql import Row, functions
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import  Pipeline
from pyspark.ml.feature import IndexToString, StringIndexer,\
    VectorIndexer, HashingTF, Tokenizer
from pyspark.ml.classification import DecisionTreeClassificationModel, DecisionTreeClassifier
from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local').appName('my app').getOrCreate()


def f(x):
    rel = {}
    rel['features'] = Vectors.\
        dense(float(x[0]), float(x[1]), float(x[2]), float(x[3]))
    rel['label'] = str(x[4])
    return rel


def f1(x):
    list1 = []
    for i in x:
        if len(i) != 0:
            list1.append(i)
    return list1


data = spark.sparkContext.\
    textFile('file:///C:/Users/Lenovo/Desktop/iris.txt').\
    map(lambda line: line.strip().split(' ')).\
    map(lambda line: f1(line)).\
    map(lambda p: Row(**f(p))).\
    toDF()

# 分别获取标签列和特征列，进行索引并进行重命名
labelIndexer = StringIndexer().\
    setInputCol('label').\
    setOutputCol('indexedLabel').\
    fit(data)

featureIndexer = VectorIndexer().\
    setInputCol('features').\
    setOutputCol('indexedFeatures').\
    setMaxCategories(4).\
    fit(data)

# 设置一个IndexToString的转换器
labelConverter = IndexToString().\
    setInputCol('prediction').\
    setOutputCol('predictedLabel').\
    setLabels(labelIndexer.labels)

trainingData, testData = data.randomSplit([0.7, 0.3])

# 构建决策树分类模型，设置决策树的参数
dtClassifier = DecisionTreeClassifier().\
    setLabelCol('indexedLabel').\
    setFeaturesCol('indexedFeatures')

# 构建机器学习流水线(Pipeline),调用fit()进行模型训练
dtPipeline = Pipeline().\
    setStages([labelIndexer, featureIndexer, dtClassifier, labelConverter])
dtPipelineModel = dtPipeline.fit(trainingData)
dtPredictions = dtPipelineModel.transform(testData)
dtPredictions.select('predictedLabel', 'label', 'features').show(20)

evaluator = MulticlassClassificationEvaluator().\
    setLabelCol('indexedLabel').\
    setPredictionCol('prediction')

dtAccuracy = evaluator.evaluate(dtPredictions)
print('决策树模型准确率：{}'.format(dtAccuracy))  # 模型的预测准确率


# 通过调用toDebugString方法查看训练的决策树模型结构
treeModelClassifier = dtPipelineModel.stages[2]
print('Learned classification tree model:\n' + str(treeModelClassifier.toDebugString))

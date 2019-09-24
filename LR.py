from pyspark.ml.linalg import Vector, Vectors
from pyspark.sql import Row, functions
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import  Pipeline
from pyspark.ml.feature import IndexToString, StringIndexer,\
    VectorIndexer, HashingTF, Tokenizer
from pyspark.ml.classification import LogisticRegression
from pyspark.sql import SparkSession
# import pandas as pd
# g = pd.read_csv('C:/Users/Lenovo/Desktop/iris.csv', header=None)
# # 转成文本文件
# with open('C:/Users/Lenovo/Desktop/iris.txt', 'w') as f:
#     g.to_string(f, index=None, header=None)

spark = SparkSession.builder.master('local').appName('word count').getOrCreate()
# 这里把特征存储在Vector中，创建一个iris模式的RDD，然后转化成dataframe，最后调用show()方法来查看一下部分数据


def f(x):
    rel = {}
    rel['features'] = Vectors.\
        dense(float(x[0]), float(x[1]), float(x[2]), float(x[3]))
    rel['label'] = str(x[4])
    return rel
# 注意remove没有返回值,所以用这种方法去除''是有问题的，因为每去除一次，x列表的长度会变
# def f(x):
#     for i in x:
#         if i is '':
#             x.remove(i)
#     return x


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
print(data)


data.show()
# 分别获取标签列和特征列，进行索引并进行重命名
labelIndexer = StringIndexer().\
    setInputCol('label').\
    setOutputCol('indexedLabel').\
    fit(data)

featureIndexer = VectorIndexer().\
    setInputCol('features').\
    setOutputCol('indexedFeatures').\
    fit(data)

# 设置LogisticRegression算法的参数
lr = LogisticRegression().\
    setLabelCol('indexedLabel').\
    setFeaturesCol('indexedFeatures').\
    setMaxIter(100).\
    setRegParam(0.3).\
    setElasticNetParam(0.8)

print('LogisticRegression parameters:\n' + lr.explainParams())

# 设置一个IndexToString的转换器
labelConverter = IndexToString().\
    setInputCol('prediction').\
    setOutputCol('predictedLabel').\
    setLabels(labelIndexer.labels)
# 把预测的类别重新转化成字符型的，构建一个机器学习流水线，设置各个阶段，上一个阶段的输出，将是本阶段的输入
lrPipeline = Pipeline().\
    setStages([labelIndexer, featureIndexer, lr, labelConverter])
# 把数据集随机分成训练集和测试集，其中训练集占70%
trainingData, testData = data.randomSplit([0.7, 0.3])
lrPipelineModel = lrPipeline.fit(trainingData)
lrPredictions = lrPipelineModel.transform(testData)
'''
Pipeline本质上是一个评估器，当Pipeline调用fit()的时候就产生了一个PipelineModel，它是一个
转换器，然后，这个PipelineModel就可以调用transform()来进行预测，生成一个新的DataFrame,即利用训练得到的
模型对测试集进行验证
'''

# 输出预测的结果
preRel = lrPredictions.select(
    'predictedLabel',\
    'label',\
    'features',\
    'probability').\
    collect()
for item in preRel:
    print(str(item['label'])+','+\
          str(item['features'])+'-->prob='+\
          str(item['probability'])+',predictedLabel'+\
          str(item['predictedLabel'])
          )

# 对训练的模型进行评估
evaluator = MulticlassClassificationEvaluator().\
    setLabelCol('indexedLabel').\
    setPredictionCol('prediction')
lrAccuracy = evaluator.evaluate(lrPredictions)
print('LR模型准确率：{}'.format(lrAccuracy))

# 可以通过model来获取训练得到的逻辑斯蒂模型
lrModel = lrPipelineModel.stages[2]
print('Coefficients:\n'+str(lrModel.coefficientMatrix) +\
      '\n Intercept:'+str(lrModel.interceptVector) +\
      '\n numClasses:'+str(lrModel.numClasses) +\
      '\n numFeatures:'+str(lrModel.numFeatures))

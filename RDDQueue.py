import time
from pyspark import SparkContext
from pyspark.streaming import StreamingContext


if __name__ == "__main__":
    sc = SparkContext(appName='python streaming')
    ssc = StreamingContext(sc, 2)
    # 创建一个队列，通过该队列可以把RDD推给一个RDD队列流
    rddQueue = []
    for i in range(5):
        rddQueue += [ssc.sparkContext.parallelize([j for j in range(1, 1000)], 10)]
        time.sleep(1)
        # 创建一个RDD队列流
        inputStream = ssc.queueStream(rddQueue)
        mappedStream = inputStream.map(lambda x: (x % 10, 1))
        reducedStream = mappedStream.reduceByKey(lambda a, b: a+b)
        reducedStream.pprint()
        ssc.start()
        ssc.stop(stopSparkContext=True, stopGraceFully=True)

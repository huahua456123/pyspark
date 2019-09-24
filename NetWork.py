from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print('Usage:Network.py<hostname><port>', file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName='python streaming wordcount')
    ssc = StreamingContext(sc, 1)
    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
    counts = lines.flatMap(lambda line: line.strip(' ')).\
        map(lambda word: (word, 1)).\
        reduceByKey(lambda a, b: a+b)
    counts.pprint()
    ssc.start()
    ssc.awaitTermination()

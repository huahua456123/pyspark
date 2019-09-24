"""
对于一个给定的文件(数据如file.txt所示)，请对数据进行排序，首先根据第一列数据降序排序
如果第一列数据相等，则根据第二列数据降序排序
"""
from pyspark import SparkContext, SparkConf
from operator import gt


class SecondarySortKey:
    def __init__(self, k):
        self.column1 = k[0]
        self.column2 = k[1]

    def __gt__(self, other):
        if other.column1 == self.column1:
            return gt(self.column2, other.column2)
        else:
            return gt(self.column1, other.column1)


def main():
    conf = SparkConf().setAppName('spark_sort').setMaster('local[1]')
    sc = SparkContext(conf=conf)
    file = 'file:///D:/Pycharm/project/file/file4.txt'
    rdd1 = sc.textFile(file)
    rdd2 = rdd1.filter(lambda x: (len(x.strip()) > 0))
    rdd3 = rdd2.map(lambda x: ((int(x.split(' ')[0]), int(x.split(' ')[1])), x))
    print(rdd3.collect())
    rdd4 = rdd3.map(lambda x: (SecondarySortKey(x[0]), x[1]))
    rdd5 = rdd4.sortByKey(False)
    rdd6 = rdd5.map(lambda x: x[1])
    rdd6.foreach(print)


if __name__ == '__main__':
    main()
    print(SecondarySortKey((5, 6)))


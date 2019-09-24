from pyspark import SparkContext, SparkConf
import shutil
# 因此如果想删除E盘下某个文件夹,这样 test 文件夹内的所有文件（包括 test 本身）都会被删除，并且忽略错误
# shutil.rmtree('E:\\myPython\\image-filter\\test', ignore_errors=True)
# path1 = os.path.dirname(os.path.abspath(__file__))
# path = path1+'/file/'
path = '///D:/Pycharm/project/file/file*.txt'

index = 0


def getindex():
    global index
    index += 1
    return index


def main():
    conf = SparkConf().setAppName('my app').setMaster('local[1]')
    sc = SparkContext(conf=conf)
    lines = sc.textFile("file:%s" % path)
    result1 = lines.filter(lambda line: (len(line.strip()) > 0))
    result2 = result1.map(lambda x: (int(x.strip()), ""))
    result3 = result2.repartition(1)
    result4 = result3.sortByKey(True)
    result5 = result4.map(lambda x: x[0])
    print(result5.collect())
    result6 = result5.map(lambda x: (getindex(), x))
    result6.foreach(print)
    result6.saveAsTextFile('file:///D:/Pycharm/project/file/sortresult')


if __name__ == "__main__":
    try:
        shutil.rmtree('D:/Pycharm/project/file/sortresult', ignore_errors=True)  # test 文件夹内的所有文件（包括 test 本身）都会被删除
    except Exception as e:
        print(e)
    main()

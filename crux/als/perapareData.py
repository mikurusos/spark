import util
from __init__ import *

data1 = sc.textFile("%s/data/male/2016031518"  %(HDFS_OUTPUT_PATH) )
data2 = sc.textFile("%s/data/male/2016031618"  %(HDFS_OUTPUT_PATH) )

data= data1.union(data2)

data.saveAsTextFile("%s/data/testSample/male/20160315-1618"  %(HDFS_OUTPUT_PATH) )

sc.stop()
# -*- coding: utf-8 -*-
from __init__ import *
from operator import add


data = sc.textFile("%s/results/parameters/female/2016031518_003" % (HDFS_OUTPUT_PATH))

output=data.map(lambda x:json.loads(x)).filter(lambda x:  x[1][1]>0.6).map(lambda x:(x[0][1],1))\
        .reduceByKey(add).map(lambda x:(x[1],1)).reduceByKey(add)

output.saveAsTextFile("%s/results/maleBeLikedDistribute/2016031518_003_30"  %(HDFS_OUTPUT_PATH) )
sc.stop()
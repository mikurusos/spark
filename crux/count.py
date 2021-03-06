# -*- coding: utf-8 -*-
import sys
import json
import cPickle as pickle
from operator import add
from pyspark import SQLContext, SparkContext, SparkConf
reload(sys)
sys.setdefaultencoding('utf-8')


from config import sc

#b = sc.broadcast(gender)

data = sc.textFile("hdfs://antispam/user/hadoop/output/chencheng/crux/data/rawData/2016032418-24/")

out = data.map(lambda x:json.loads(x)).flatMap(lambda x:x[0])\
        .map(lambda x:(x,1))\
        .reduceByKey(lambda x,y:x).map(lambda x:x[0]).collect()


with open('/home/hadoop/chen.cheng/moa/0324_momoid', 'w') as f:
    for item in out:
        f.write("%s\n" %( item  ) )
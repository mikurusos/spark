# -*- coding: utf-8 -*-
import sys
import json
import math
from operator import add
#from pyspark import SQLContext, SparkContext, SparkConf
reload(sys)
sys.setdefaultencoding('utf-8')


keyword='book'
'''
conf = SparkConf().setAppName("chencheng's task").setMaster("spark://anti-spam-spark-001.yz.momo.com:8081,anti-spam-spark-002.yz.momo.com:8081")
sc = SparkContext(conf=conf)

data = sc.textFile("hdfs://antispam/user/wang.fangkui/hobby.res")

output = data.map(lambda x: x.split('\t') ) \
        .map(lambda x: [x[0], json.loads(x[1])]).filter(lambda x: x[1])\
        .filter(lambda x: keyword in x[1] and x[1][keyword]) \
        .map(lambda x: (x[0], x[1][keyword].split(',')))\
        .filter(lambda x:len(x[1])>0).map(lambda x: json.dumps(x))

output.saveAsTextFile("hdfs://antispam/user/hadoop/output/chencheng/crux/data/bobby/%s" % (keyword))

sc.stop()
'''
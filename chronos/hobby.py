# -*- coding: utf-8 -*-
import sys
import json
import math
from operator import add
from pyspark import SQLContext, SparkContext, SparkConf
reload(sys)
sys.setdefaultencoding('utf-8')


conf = SparkConf().setAppName("chencheng's task").setMaster("spark://anti-spam-spark-001.yz.momo.com:8081,anti-spam-spark-002.yz.momo.com:8081")
sc = SparkContext(conf=conf)

data = sc.textFile("hdfs://antispam/user/wang.fangkui/hobby.res")

output = map(lambda x: x.split('\t') ) \
        .map(lambda x: [x[0], json.loads(x[1])]).filter(lambda x: x[1])\
        .filter(lambda x: 'music' in x[1] and x[1]['music']) \
        .map(lambda x: (x[0], len(x[1]['music'].split(',')))).collect()

with open('/home/hadoop/chen.cheng/Chronos/hobby_count', 'w') as f:
    for item in output:
        f.write("%s\t%d\n" %( item[0], item[1]  ) )
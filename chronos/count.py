# -*- coding: utf-8 -*-
import sys
import json
import math
from operator import add
from pyspark import SQLContext, SparkContext, SparkConf
reload(sys)
sys.setdefaultencoding('utf-8')


keyword='book'

conf = SparkConf().setAppName("chencheng's task").setMaster("spark://anti-spam-spark-001.yz.momo.com:8081,anti-spam-spark-002.yz.momo.com:8081")
sc = SparkContext(conf=conf)

data = sc.textFile("hdfs://antispam/user/hadoop/output/wang.yuqi/Venus/like_person/2016030218-24/")

output = data.map(lambda x : x.split('\t')).flatMap(lambda x: json.loads(json.loads(x[0]))) \
        .map(lambda x: (x,1)).reduceByKey(lambda x,y:x).collect()

with open('/home/hadoop/chen.cheng/Chronos/0302_count', 'w') as f:
    for item in output:
        f.write("%s\n" %( item[0]  ) )
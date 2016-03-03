# -*- coding: utf-8 -*-
import sys
import json
import cPickle as pickle
from operator import add
from pyspark import SQLContext, SparkContext, SparkConf
reload(sys)
sys.setdefaultencoding('utf-8')

conf = SparkConf().setAppName("chencheng's task").setMaster("spark://anti-spam-spark-001.yz.momo.com:8081,anti-spam-spark-002.yz.momo.com:8081")
sc = SparkContext(conf=conf)

data = sc.textFile("hdfs://antispam/user/hadoop/output/wang.yuqi/Venus/like_person/2016030218-24/")

out = data.map(lambda x : x.split('\t')).map(lambda x: [json.loads(json.loads(x[0])), json.loads(x[1])[0]])\
        .collect()

with open('/home/hadoop/chen.cheng/Chronos/0302_data', 'w') as f:
    for item in out:
        f.write("%s\t%s\t%d\n" %(item[0][0], item[0][1], item[1]) )
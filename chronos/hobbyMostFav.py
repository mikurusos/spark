# -*- coding: utf-8 -*-
import sys
import json
from operator import add
from pyspark import SQLContext, SparkContext, SparkConf
reload(sys)
sys.setdefaultencoding('utf-8')


keyword='book'

conf = SparkConf().setAppName("chencheng's task").setMaster("spark://anti-spam-spark-001.yz.momo.com:8081,anti-spam-spark-002.yz.momo.com:8081")
sc = SparkContext(conf=conf)

data = sc.textFile("hdfs://antispam/user/wang.fangkui/hobby.res")

output = data.map(lambda x: x.split('\t') ) \
        .map(lambda x: [x[0], json.loads(x[1])]).filter(lambda x: x[1])\
        .filter(lambda x: keyword in x[1] and x[1][keyword]) \
        .map(lambda x: (x[0], x[1][keyword].split(','))) \
        .flatMap(lambda x: [(int(i), 1) for i in x[1]]).reduceByKey(add)  \
        .collect()

with open('/home/hadoop/chen.cheng/Chronos/most_fav_book', 'w') as f:
    for item in output:
        f.write("%d\t%d\n" %( item[0], item[1]  ) )
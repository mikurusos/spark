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


def var(l):
    s1= sum([i[0]  for i in l])/len(l)
    s2= sum([i[1]  for i in l])/len(l)
    var = sum( [ math.fabs(i[0] - s1)+ math.fabs(i[1] - s2) for i in l ]  )/len(l)
    return var


data = sc.textFile("hdfs://antispam/user/hadoop/output/wang.yuqi/crux/wheretheymeet/2016030116-24/")

output= data.map(lambda x: x.split('\t')).map(lambda x: (json.loads(x[0]), json.loads(x[1]))) \
        .map(lambda x: (x[0], [item[0] for item in x[1]])).filter(lambda x: var(x[1])>3).count()

#output = data.collect()

with open('result', 'w') as f:
    for item in output:
        f.write("%s\t%s\n" %( item[0], str(item[1])  ) )



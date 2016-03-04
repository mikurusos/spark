# -*- coding: utf-8 -*-
import sys
import json
import cPickle as pickle
from operator import add
from pyspark import SQLContext, SparkContext, SparkConf
reload(sys)
sys.setdefaultencoding('utf-8')

try:
    print "loading..."
    with open("/home/hadoop/chen.cheng/moa/gender.pkl", "rb") as f:
        gender = pickle.load(f)
    print "finished!"
except:
    gender = {}

conf = SparkConf().setAppName("chencheng's task").setMaster("spark://anti-spam-spark-001.yz.momo.com:8081,anti-spam-spark-002.yz.momo.com:8081")
sc = SparkContext(conf=conf)

b = sc.broadcast(gender)

data = sc.textFile("hdfs://antispam/user/hadoop/output/wang.yuqi/Venus/like_person/2016030218-24/")

output = data.map(lambda x : x.split('\t')).flatMap(lambda x: json.loads(json.loads(x[0])))\
        .map(lambda x: [x,1]).reduceByKey(lambda x,y:x).map(lambda x:x[0]) \
        .collect()

'''
male = tmp.filter(lambda x: x[0][0].isdigit()).filter(lambda x: int(x[0][0]) in gender and gender[int(x[0][0])]=='M').count()
female = tmp.filter(lambda x: x[0][0].isdigit()).filter(lambda x: int(x[0][0]) in gender and gender[int(x[0][0])]=='F').count()
'''

with open('/home/hadoop/chen.cheng/Chronos/0303_momoid', 'w') as f:
    for item in output:
        f.write("%s\n" %( item  ) )
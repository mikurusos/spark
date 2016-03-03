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

try:
    print "loading..."
    with open("/home/hadoop/chen.cheng/moa/gender2.pkl", "rb") as f:
        gender = pickle.load(f)
    print "finished!"
except:
    gender = {}

b = sc.broadcast(gender)

data = sc.textFile("hdfs://antispam/user/hadoop/output/chencheng/crux/data/2016030218-24/")

out = data.map(lambda x : json.loads(x)).filter(lambda x: gender[x[0][0]] == "M")

'''
with open('/home/hadoop/chen.cheng/Chronos/momoid', 'w') as f:
    for item in out:
        f.write("%s\n" %(item ) )
'''

out.saveAsTextFile("hdfs://antispam/user/hadoop/output/chencheng/crux/data/2016030218-24_male/")

sc.stop()
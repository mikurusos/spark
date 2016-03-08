# -*- coding: utf-8 -*-
import sys
import json
from operator import add
reload(sys)
sys.setdefaultencoding('utf-8')

from config import sc
from util import loadPickle

gender = loadPickle("/home/hadoop/chen.cheng/moa/gender4.pkl")
b = sc.broadcast(gender)
data = sc.textFile("hdfs://antispam/user/hadoop/output/chencheng/crux/data/rawData/2016030618-24/")

out_male = data.map(lambda x : json.loads(x)).filter(lambda x: x[0][0] and x[0][1])\
    .filter(lambda x: b.value[int(x[0][0])] == "M").map(lambda x: json.dumps(x))

out_female = data.map(lambda x : json.loads(x)).filter(lambda x: x[0][0] and x[0][1])\
    .filter(lambda x: b.value[int(x[0][0])] == "F")\
        .map(lambda x: json.dumps(x))

'''
with open('/home/hadoop/chen.cheng/Chronos/momoid', 'w') as f:
    for item in out:
        f.write("%s\n" %(item ) )
'''

out_male.saveAsTextFile("hdfs://antispam/user/hadoop/output/chencheng/crux/data/male/2016030618/")
out_female.saveAsTextFile("hdfs://antispam/user/hadoop/output/chencheng/crux/data/female/2016030618/")
# -*- coding: utf-8 -*-
import sys
import json
from operator import add
reload(sys)
sys.setdefaultencoding('utf-8')

from config import sc
from util import loadPickle

date = sys.argv[1]

gender = loadPickle("/home/hadoop/chen.cheng/moa/gender11.pkl")
b = sc.broadcast(gender)
data = sc.textFile("hdfs://antispam/user/hadoop/output/chencheng/crux/data/rawData/%s-24/" % (date))

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

out_male.saveAsTextFile("hdfs://antispam/user/hadoop/output/chencheng/crux/data/male/%s/"% (date))
out_female.saveAsTextFile("hdfs://antispam/user/hadoop/output/chencheng/crux/data/female/%s/"% (date))
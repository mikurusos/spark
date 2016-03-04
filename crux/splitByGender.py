# -*- coding: utf-8 -*-
import sys
import json
from operator import add
reload(sys)
sys.setdefaultencoding('utf-8')

from config import sc
from util import loadPickle

gender = loadPickle("/home/hadoop/chen.cheng/moa/gender3.pkl")
b = sc.broadcast(gender)
data = sc.textFile("hdfs://antispam/user/hadoop/output/chencheng/crux/data/2016030218-24/")

out = data.map(lambda x : json.loads(x)).filter(lambda x: int(x[0][0]) in gender and gender[int(x[0][0])] == "M")\
        .map(lambda x: json.dumps(x))

'''
with open('/home/hadoop/chen.cheng/Chronos/momoid', 'w') as f:
    for item in out:
        f.write("%s\n" %(item ) )
'''

out.saveAsTextFile("hdfs://antispam/user/hadoop/output/chencheng/crux/data/2016030218-24_male/")

sc.stop()
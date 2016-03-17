# -*- coding: utf-8 -*-
from __init__ import *
import numpy as np

threshold = float(sys.argv[1])

tmp_like=0
tmp_dislike=0
like=sc.accumulator(0)
dislike=sc.accumulator(0)

def myCount(x):
    global like, dislike
    if(x[0]==1):
        like+=1
    else:
        dislike+=1
    return x

result={}

data = sc.textFile("%s/results/parameters/female/2016031518_004" % (HDFS_OUTPUT_PATH))

tmp=data.map(lambda x:json.loads(x)).map(lambda x:(int(x[1][0]), x[1][1]))
tmp.cache()

for i in np.arange(0,1,0.1):
    tmp.filter(lambda x:x[1]>i).map(myCount).count()
    result[i] = (like.value - tmp_like, dislike.value- tmp_dislike)
    tmp_like=like.value
    tmp_dislike = dislike.value


with open('/home/hadoop/chen.cheng/Chronos/parameters/AUC_purity', 'w') as f:
    for item in result:
        f.write("%d\t%d\t%d\n" %(item, result[item][0], result[item][1] ) )
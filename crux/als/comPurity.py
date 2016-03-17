# -*- coding: utf-8 -*-
from __init__ import *

threshold = float(sys.argv[1])

like=sc.accumulator(0)
dislike=sc.accumulator(0)

def myCount(x):
    global like, dislike
    if(x[0]==1):
        like+=1
    else:
        dislike+=1
    return x

data = sc.textFile("%s/results/parameters/female/2016031518_004" % (HDFS_OUTPUT_PATH))

data.map(lambda x:json.loads(x)).map(lambda x:(int(x[1][0]), x[1][1]))\
    .filter(lambda x:x[1]>threshold).map(myCount).count()


with open('/home/hadoop/chen.cheng/Chronos/parameters/AUC_purity', 'w') as f:
    f.write("%d\t%d\n" %( like.value, dislike.value ) )
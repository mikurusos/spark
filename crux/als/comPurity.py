# -*- coding: utf-8 -*-
from __init__ import *
import numpy as np

#threshold = float(sys.argv[1])

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

data = sc.textFile("%s/results/female/2016032218_0310-21" % (HDFS_OUTPUT_PATH))

tmp=data.map(lambda x:json.loads(x)).map(lambda x:(int(x[1][0]), x[1][1]))
tmp.cache()

for i in np.arange(-0.5,1.8,0.1):
    tmp.filter(lambda x:i-0.1<=x[1]<i).map(myCount).count()
    result["%f-%f"%(i-0.1, i)] = (like.value - tmp_like, dislike.value- tmp_dislike)
    tmp_like=like.value
    tmp_dislike = dislike.value


with open('/home/hadoop/chen.cheng/Chronos/parameters/AUC_purity_female_dislike_2016032218_0310-21', 'w') as f:
    for item in result:
        f.write("%s\t%d\t%d\t%f\n" %(item, result[item][0], result[item][1], float(result[item][0])/result[item][1] ) )
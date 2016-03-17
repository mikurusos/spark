import sys
from __init__ import *

like=sc.accumulator(0)
dislike=sc.accumulator(0)

def myCount(x):
    global like, dislike
    if(x):
        like+=1
    else:
        dislike+=1
    return x

data = sc.textFile("%s/results/parameters/female/2016031518_004" % (HDFS_OUTPUT_PATH))

data.map(lambda x:json.loads(x)).map(lambda x:(int(x[1][0]), x[1][1]))\
    .filter(lambda x:x[1]>int(sys.argv[1])).map(myCount).count()


with open('/home/hadoop/chen.cheng/Chronos/parameters/AUC_purity', 'w') as f:
    f.write("%d\t%d\n" %( like.value, dislike.value ) )
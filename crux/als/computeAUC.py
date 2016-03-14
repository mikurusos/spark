from __init__ import *

totalCount=sc.accumulator(0)
invalidCount=sc.accumulator(0)

def getThreshold(x):
    global totalCount
    totalCount+=1
    return int(x[0]), 1 if(x[1]>0.7) else 0

def getInvalid(x):
    global invalidCount
    if(x[0]==x[1]):
        invalidCount+=1
        return 1
    else:
        return 0

data = sc.textFile("hdfs://antispam/user/hadoop/output/chencheng/crux/results/2016030318/")

data.map(lambda x:json.loads(x)).map(lambda x:(int(x[1][0]), x[1][1])).map(getThreshold)\
    .filter(getInvalid).count()

with open('/home/hadoop/chen.cheng/Chronos/AUC', 'w') as f:
    f.write("%d\t%d\n" %( totalCount.value, invalidCount.value ) )
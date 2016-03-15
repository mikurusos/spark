from __init__ import *

totalTrue=sc.accumulator(0)
totalFalse=sc.accumulator(0)
invalidTrue=sc.accumulator(0)
invalidFalse=sc.accumulator(0)

def getThreshold(x):
    global totalTrue, totalFalse
    res = x[0], 1 if(x[1]>0.5) else 0
    if(res[1]):
        totalTrue+=1
    else:
        totalFalse+=1
    return res

def getInvalid(x):
    global invalidTrue, invalidFalse
    if(x[0]==1):
        if(x[0]==x[1]):
            invalidTrue+=1
            return 1
        else:
            return 0
    else:
        if(x[0]==x[1]):
            invalidFalse+=1
            return 1
        else:
            return 0

data = sc.textFile("%s/results/male/2016030818/" % (HDFS_OUTPUT_PATH))

data.map(lambda x:json.loads(x)).map(lambda x:(int(x[1][0]), x[1][1])).map(getThreshold)\
    .filter(getInvalid).count()

with open('/home/hadoop/chen.cheng/Chronos/AUC', 'w') as f:
    f.write("%d\t%d\t%d\t%d\n" %( totalTrue.value, totalFalse.value, invalidTrue.value, invalidFalse.value ) )
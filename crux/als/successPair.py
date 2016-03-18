from __init__ import *

def pairAdd(x,y):
    return x[0]+y[0], x[1]+y[1]

male = sc.textFile("%s/data/male/2016031518"  %(HDFS_OUTPUT_PATH) ).map(lambda x:json.loads(x))
female = sc.textFile("%s/data/female/2016031518"  %(HDFS_OUTPUT_PATH) ).map(lambda x:json.loads(x))

femaleData= female.map(lambda x: ((x[0][1],x[0][0]), (x[1],1) ))
maleData= male.map(lambda x: ((x[0][0],x[0][1]),(x[1],1) ))

pair = maleData.union(femaleData).reduceByKey(pairAdd).cache()

totalPair = pair.filter(lambda x:x[1][1]>1).count()
successedPair = pair.filter(lambda x:x[1][0]>1 and x[1][1]>1).count()

with open('/home/hadoop/chen.cheng/Chronos/successedPair_2016031518', 'w') as f:
    f.write("%d\t%d" %(totalPair, successedPair) )
from __init__ import *
from operator import add

rawData=sc.textFile("hdfs://antispam/user/hadoop/output/chencheng/crux/data/female/2016031[3-9]18")

rawData = rawData.map(lambda x:json.loads(x))\
    .map(lambda x:(int(x[0][0]), int(x[0][1])))\
    .map(lambda x:(x,1))

data=sc.textFile("hdfs://antispam/user/hadoop/output/chencheng/crux/data/female/2016032018")

data = data.map(lambda x:json.loads(x))\
    .map(lambda x:(int(x[0][0]), int(x[0][1])))\
    .map(lambda x:(x,1))

total  = data.union(rawData).reduceByKey(add)

num1= total.filter(lambda x:x[1]==2).count()
num2= data.count()

with open('/home/hadoop/chen.cheng/Chronos/coverRate', 'w') as f:
    f.write("%d\t%d\n" %(num1, num2 ) )
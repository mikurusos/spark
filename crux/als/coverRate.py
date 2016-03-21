from __init__ import *
from operator import add

rawData=sc.textFile("hdfs://antispam/user/hadoop/output/chencheng/crux/results/female/2016031[2-8]18")

rawData = rawData.map(lambda x:json.loads(x))\
    .map(lambda x:(int(x[0][0]), int(x[0][1]))).cache()

rawMale = rawData.map(lambda x:(x[0],1)).distinct()
rawFemale = rawData.map(lambda x:(x[1],1)).distinct()

data=sc.textFile("hdfs://antispam/user/hadoop/output/chencheng/crux/results/female/2016031918")

data = data.map(lambda x:json.loads(x))\
    .map(lambda x:(int(x[0][0]), int(x[0][1]))).cache()

male = rawData.map(lambda x:(x[0],1)).distinct()
num1_male=male.count()
female = rawData.map(lambda x:(x[1],1)).distinct()
num1_female=female.count()

num2_male=male.union(rawMale).reduceByKey(add).filter(lambda x:x[1]==2).count()
num2_female=female.union(rawFemale).reduceByKey(add).filter(lambda x:x[1]==2).count()

with open('/home/hadoop/chen.cheng/Chronos/coverRate', 'w') as f:
    f.write("%d\t%d\t%d\t%d\n" %(num1_male, num1_female,num2_male,num2_female ) )
import sys
import json
from operator import add
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from pyspark import SQLContext, SparkContext, SparkConf
reload(sys)
sys.setdefaultencoding('utf-8')

from crux.config import sc
import os
curPath = os.path.abspath(os.path.dirname(__file__))
print curPath
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)

from pyspark.sql import SQLContext, Row
sqlContext = SQLContext(sc)

totalCount=sc.accumulator(0)
invalidCount=sc.accumulator(0)

def getThreshold(x):
    global totalCount
    totalCount+=1
    return 1 if(x>0.6) else 0

def getInvalid(x):
    global invalidCount
    if(x[0]==x[1]):
        invalidCount+=1
        return 1
    else:
        return 0

model = MatrixFactorizationModel.load(sc,"hdfs://antispam/user/hadoop/output/chencheng/model/als_female_2-7")
schema = sqlContext.read.load("hdfs://antispam/user/hadoop/output/chencheng/crux/data/dataFrame/2016030618")


com= schema.map(lambda x: (model.predict(x.sender, x.receivor), x.like)).map(lambda x: (getThreshold(x[0]), x[1]))
invalid= com.filter(lambda x: getInvalid(x))

with open('/home/hadoop/chen.cheng/Chronos/AUC', 'w') as f:
    f.write("%d\t%d\n" %( totalCount,  invalidCount ) )
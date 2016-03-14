import sys
import json
from operator import add
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from pyspark import SQLContext, SparkContext, SparkConf
reload(sys)
sys.setdefaultencoding('utf-8')

conf = SparkConf().setAppName("chencheng's task").setMaster("spark://anti-spam-spark-001.yz.momo.com:8081,anti-spam-spark-002.yz.momo.com:8081")
sc = SparkContext(conf=conf)

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

# load the dataframe
data = sc.textFile("hdfs://antispam/user/hadoop/output/chencheng/crux/data/rawData/2016030618-24/")

rawData=data.map(lambda x:json.loads(x)).map(lambda x:((int(x[0][0]), int(x[0][1])),x[1])).filter(lambda x: x[0][0] and x[0][1] and x[1])
rawData.cache()

#predict the results
prediction = model.predictAll(rawData.map(lambda x:x[0])).map(lambda x:((x.user, x.product), x.rating))

# combining with the real
combins = rawData.join(prediction).map(lambda x: json.dumps(x))


combins.saveAsTextFile("hdfs://antispam/user/hadoop/output/chencheng/crux/results/2016030618")

sc.stop()


'''
predictionAndLabels = schema.map(lambda x: x.like).zip(prediction)


com= predictionAndLabels.map(lambda x: (getThreshold(x[1]), x[0]))
invalid= com.filter(lambda x: getInvalid(x))

with open('/home/hadoop/chen.cheng/Chronos/AUC', 'w') as f:
    f.write("%d\t%d\n" %( totalCount.value,  invalidCount.value ) )
'''
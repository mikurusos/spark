import sys
import json
from operator import add
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from pyspark import SQLContext, SparkContext, SparkConf
reload(sys)
sys.setdefaultencoding('utf-8')

conf = SparkConf().setAppName("chencheng's task").setMaster("spark://anti-spam-spark-001.yz.momo.com:8081,anti-spam-spark-002.yz.momo.com:8081")
sc = SparkContext(conf=conf)

user_artist_data = sc.textFile("hdfs://antispam/user/hadoop/output/chencheng/crux/data/2016030218-24/")

#data = user_artist_data.map(lambda l: l.split()).filter(lambda x: len(x)==2)\
#    .map(lambda x:(x[0], json.loads(x[1]))).filter(lambda x: x[0].isdigit() and x[1].isdigit() )\
#    .map(lambda l: (int(l[0]), int(l[1])))
#data.cache()

#momoids = data.map(lambda x:(x[0],1)).reduceByKey(add).filter(lambda x: 10< x[1]<50).map(lambda x: x[0]).collect()
#b = sc.broadcast(momoids)


#ratings = user_artist_data.map(lambda l: l.split('\t',1)).map(lambda x: (x[0], json.loads(x[1]))) \
#    .flatMap(lambda x: [[x[0], json.loads(item)]  for item in x[1]]) \
#    .filter(lambda x: x[0].isdigit()) \
#    .map(lambda l: Rating(int(l[0]), int(l[1]), 1))
#ratings.cache()

ratings = user_artist_data.map(lambda l: l.split()).filter(lambda x: len(x)==2)\
    .map(lambda x: Rating(int(x[0]), int(x[1]), int(x[2])))
ratings.cache()

rank = 10
numIterations = 10
model = ALS.trainImplicit(ratings, rank, numIterations, alpha=0.01)

#for item in model.recommendProducts(2093760, 5):
#    print artist_data[str(item[1])]

model.save(sc,"hdfs://antispam/user/hadoop/output/chencheng/model/als_male2female")
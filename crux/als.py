import sys
import json
from operator import add
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from pyspark import SQLContext, SparkContext, SparkConf
reload(sys)
sys.setdefaultencoding('utf-8')

conf = SparkConf().setAppName("chencheng's task").setMaster("spark://anti-spam-spark-001.yz.momo.com:8081,anti-spam-spark-002.yz.momo.com:8081")
sc = SparkContext(conf=conf)
sc.setCheckpointDir("hdfs://antispam/user/hadoop/output/chencheng/checkpoint")

#user_artist_data1 = sc.textFile("hdfs://antispam/user/hadoop/output/chencheng/crux/data/female/2016030[2-9]18/")
user_artist_data = sc.textFile("hdfs://antispam/user/hadoop/output/chencheng/crux/data/female/2016031[0-9]18/")

#user_artist_data= user_artist_data1.union(user_artist_data2)

ratings = user_artist_data.map(lambda x: json.loads(x))\
        .filter(lambda x: x[0][0] and x[0][1])\
        .map(lambda x: Rating(int(x[0][0]), int(x[0][1]), float(x[1])))
ratings.checkpoint()
ratings.cache()

rank = 30
numIterations = 25

ALS.setCheckpointInterval(2)
model = ALS.train(ratings, rank, numIterations,lambda_=0.03)

model.save(sc,"hdfs://antispam/user/hadoop/output/chencheng/model/als_female_parameters/30/als_female_0310-19_003")
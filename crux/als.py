import sys
import json
from operator import add
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from pyspark import SQLContext, SparkContext, SparkConf
reload(sys)
sys.setdefaultencoding('utf-8')

conf = SparkConf().setAppName("chencheng's task").setMaster("spark://anti-spam-spark-001.yz.momo.com:8081,anti-spam-spark-002.yz.momo.com:8081")
sc = SparkContext(conf=conf)

user_artist_data = sc.textFile("hdfs://antispam/user/hadoop/output/chencheng/crux/data/male/2016030[5-8]18/")

ratings = user_artist_data.map(lambda x: json.loads(x))\
        .filter(lambda x: x[0][0] and x[0][1])\
        .map(lambda x: Rating(int(x[0][0]), int(x[0][1]), float(x[1])))
ratings.cache()

rank = 20
numIterations = 25
model = ALS.train(ratings, rank, numIterations)

model.save(sc,"hdfs://antispam/user/hadoop/output/chencheng/model/als_male_0305-8")
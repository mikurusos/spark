import sys
import json
from crux.util import loadPickle
from operator import add
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from pyspark import SQLContext, SparkContext, SparkConf
reload(sys)
sys.setdefaultencoding('utf-8')

conf = SparkConf().setAppName("chencheng's task").setMaster("spark://anti-spam-spark-001.yz.momo.com:8081,anti-spam-spark-002.yz.momo.com:8081")
sc = SparkContext(conf=conf)

user_artist_data = sc.textFile("hdfs://antispam/user/hadoop/output/chencheng/crux/data/male/2016030618/")

ratings = user_artist_data.map(lambda x: json.loads(x))\
        .map(lambda x: Rating(x[0][0], x[0][1], float(x[1])))
ratings.cache()

rank = 10
numIterations = 20
model = ALS.trainImplicit(ratings, rank, numIterations ,alpha=0.1)

model.save(sc,"hdfs://antispam/user/hadoop/output/chencheng/model/als_female")
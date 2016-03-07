import sys
import json
from util import loadPickle
from operator import add
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from pyspark import SQLContext, SparkContext, SparkConf
reload(sys)
sys.setdefaultencoding('utf-8')

conf = SparkConf().setAppName("chencheng's task").setMaster("spark://anti-spam-spark-001.yz.momo.com:8081,anti-spam-spark-002.yz.momo.com:8081")
sc = SparkContext(conf=conf)

user_artist_data = sc.textFile("hdfs://antispam/user/hadoop/output/chencheng/crux/data/bobby/book/")

#tfidf=loadPickle('/home/hadoop/chen.cheng/moa/book_tfidf.pkl')
#b = sc.broadcast(tfidf)

ratings = user_artist_data.map(lambda x: json.loads(x))\
        .flatMap(lambda x: [[x[0], item] for item in x[1]]) \
        .filter(lambda x: x[0] and x[1] )\
        .map(lambda x: Rating(int(x[0]), int(x[1]), 1))
ratings.cache()

rank = 10
numIterations = 20
model = ALS.trainImplicit(ratings, rank, numIterations ,alpha=10)

model.save(sc,"hdfs://antispam/user/hadoop/output/chencheng/model/als_book_alpha=1")
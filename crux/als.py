import sys
import json
from operator import add
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from pyspark import SQLContext, SparkContext, SparkConf
reload(sys)
sys.setdefaultencoding('utf-8')

conf = SparkConf().setAppName("chencheng's task").setMaster("spark://anti-spam-spark-001.yz.momo.com:8081,anti-spam-spark-002.yz.momo.com:8081")
sc = SparkContext(conf=conf)

user_artist_data1 = sc.textFile("hdfs://antispam/user/hadoop/output/chencheng/crux/data/female/2016030[7-9]18/")
user_artist_data2 = sc.textFile("hdfs://antispam/user/hadoop/output/chencheng/crux/data/female/2016031018/")

user_artist_data= user_artist_data1.union(user_artist_data2)

ratings = user_artist_data.map(lambda x: json.loads(x))\
        .filter(lambda x: x[0][0] and x[0][1])\
        .map(lambda x: Rating(int(x[0][0]), int(x[0][1]), float(x[1])))
ratings.cache()

rank = 25
numIterations = 20
model = ALS.train(ratings, rank, numIterations)

model.save(sc,"hdfs://antispam/user/hadoop/output/chencheng/model/als_female_0307-10")
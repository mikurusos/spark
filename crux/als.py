import sys
import json
from operator import add
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from pyspark import SQLContext, SparkContext, SparkConf
reload(sys)
sys.setdefaultencoding('utf-8')

conf = SparkConf().setAppName("chencheng's task").setMaster("spark://anti-spam-spark-001.yz.momo.com:8081,anti-spam-spark-002.yz.momo.com:8081")
sc = SparkContext(conf=conf)

#user_artist_data1 = sc.textFile("hdfs://antispam/user/hadoop/output/chencheng/crux/data/female/2016030[8-9]18/")
user_artist_data = sc.textFile("hdfs://antispam/user/hadoop/output/chencheng/crux/data/male/2016031[2-4]18/")

#user_artist_data= user_artist_data1.union(user_artist_data2)

ratings = user_artist_data.map(lambda x: json.loads(x))\
        .filter(lambda x: x[0][0] and x[0][1])\
        .map(lambda x: Rating(int(x[0][0]), int(x[0][1]), float(x[1])))
ratings.cache()

rank = 30
numIterations = 20
model = ALS.train(ratings, rank, numIterations,lambda_=0.03)

model.save(sc,"hdfs://antispam/user/hadoop/output/chencheng/model/als_male_parameters/30/als_male_0312-14_003")
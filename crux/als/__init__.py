import sys
import json
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from pyspark import SQLContext, SparkContext, SparkConf
from pyspark.sql import SQLContext, Row
#from computeAUC import *
reload(sys)
sys.setdefaultencoding('utf-8')

conf = SparkConf().setAppName("chencheng's task").setMaster("spark://anti-spam-spark-001.yz.momo.com:8081,anti-spam-spark-002.yz.momo.com:8081")\
    .set("spark.driver.cores", 10)\
    .set("spark.driver.memory", "2g")\
    .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")\
    .set("spark.kryoserializer.buffer.mb", "1024")
sc = SparkContext(conf=conf)



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
    .set("spark.driver.memory", "3g")\
    .set("spark.default.parallelism", 40)\
    .set("spark.dynamicAllocation.enabled","true")\
    .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")\
    .set("spark.kryoserializer.buffer.mb", "1024")
sc = SparkContext(conf=conf)
sc.setCheckpointDir("hdfs://antispam/user/hadoop/output/chencheng/checkpoint")

HDFS_HOME_PATH="hdfs://antispam/user/hadoop/output/chencheng"

HDFS_OUTPUT_PATH="hdfs://antispam/user/hadoop/output/chencheng/crux"

HDFS_OUTPUT_RESULTS_PATH="hdfs://antispam/user/hadoop/output/chencheng/crux"


#.set("spark.shuffle.manager","hash")\



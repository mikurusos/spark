import util
from __init__ import *

female = sc.textFile("%s/data/realdata/chengdu_female0322"  %(HDFS_OUTPUT_PATH) )

with open("/home/hadoop/data/chengdu_male0322") as f:
    male= [int(i) for i in f.readlines()]

b = sc.broadcast(male)

result = female.flatMap(lambda x: [(x,i) for i in b.value])

result.saveAsTextFile("%s/data/realdata/chengdu_result0322"  %(HDFS_OUTPUT_PATH) )

sc.stop()
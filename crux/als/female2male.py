import util
from __init__ import *

female = sc.textFile("%s/data/realdata/chengdu_female0322"  %(HDFS_OUTPUT_PATH) , 100)

with open("/home/hadoop/data/chengdu_male0322") as f:
    male= [int(i) for i in f.readlines()]

b = sc.broadcast(male)

result = female.flatMap(lambda x: [(int(x),i) for i in b.value],preservesPartitioning=True)

result.saveAsTextFile("%s/data/realdata/chengdu_result0322_"  %(HDFS_OUTPUT_PATH) )

sc.stop()
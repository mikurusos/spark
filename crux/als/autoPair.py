from __init__ import *

def pairAdd(x,y):
    return x[0]+y[0], (x[1][0]+y[1][0],  x[1][1]+y[1][1])


female_results= sc.textFile("%s/data/testSample/female/20160315-1618_results_als_female_0311-14_30_003"  %(HDFS_OUTPUT_PATH) )

male_results= sc.textFile("%s/data/testSample/male/20160315-1618_results_als_male_0312-14_30_003"  %(HDFS_OUTPUT_PATH) )

femaleData= female_results.map(lambda x:json.loads(x))\
    .map(lambda x: ((x[0][1],x[0][0]),(1, (0,x[1][1])) ))

maleData= male_results.map(lambda x:json.loads(x))\
    .map(lambda x: (x[0],(1, (x[1][1],0)) ))

pair = maleData.union(femaleData).reduceByKey(pairAdd).map(lambda x:json.loads(x))

pair.saveAsTextFile("%s/data/testSample/pair/20160315-1618_results"  %(HDFS_OUTPUT_PATH) )

sc.stop()
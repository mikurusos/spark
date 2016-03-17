from __init__ import *

data = sc.textFile("%s/data/female/2016031518"  %(HDFS_OUTPUT_PATH) )

model = MatrixFactorizationModel.load(sc,"%s/model/als_female_parameters/25/als_female_0310-14_003"  %(HDFS_HOME_PATH))

data2predict=data.map(lambda x:json.loads(x)).filter(lambda x: x[0][0] and x[0][1])\
    .map(lambda x:((int(x[0][0]), int(x[0][1])),x[1]))
data2predict.cache()

#female1=data2predict.map(lambda x:(x[0][0],1)).reduceByKey(lambda x,y:x).count()

#female1 = data2predict.map(lambda x:(x,1)).reduceByKey(lambda x,y:x).count()

#predict the results
prediction = model.predictAll(data2predict.map(lambda x:x[0])).map(lambda x:((x.user, x.product), x.rating))\
    .map(lambda x:(x[0][1],1)).reduceByKey(lambda x,y:x).count()

#female2 = prediction.reduceByKey(lambda x,y:x).count()

#female2= prediction.count()

#combining with the real results
#combins = data2predict.join(prediction).map(lambda x:(x,1))\
#        .reduceByKey(lambda x,y:x).map(lambda x: json.dumps(x[0]))
#
#combins.saveAsTextFile("%s/results/parameters/female/25/2016031518_003_"  %(HDFS_OUTPUT_PATH) )

with open('/home/hadoop/chen.cheng/Chronos/female_2016031518_2', 'w') as f:
    f.write("%d" %(prediction))


sc.stop()
from __init__ import *

totalCount=sc.accumulator(0)
invalidCount=sc.accumulator(0)

data = sc.textFile("%s/data/female/2016031518"  %(HDFS_OUTPUT_PATH) )

model = MatrixFactorizationModel.load(sc,"%s/model/als_female_parameters/als_female_0311-14_30_004"  %(HDFS_HOME_PATH))

data2predict=data.map(lambda x:json.loads(x)).filter(lambda x: x[0][0] and x[0][1])\
    .map(lambda x:((int(x[0][0]), int(x[0][1])),x[1]))
data2predict.cache()

#predict the results
prediction = model.predictAll(data2predict.map(lambda x:x[0])).map(lambda x:((x.user, x.product), x.rating))

#combining with the real results
combins = data2predict.join(prediction).map(lambda x:(x,1))\
        .reduceByKey(lambda x,y:x).map(lambda x: json.dumps(x[0]))

combins.saveAsTextFile("%s/results/parameters/female/2016031518_004"  %(HDFS_OUTPUT_PATH) )
sc.stop()
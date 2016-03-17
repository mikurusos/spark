from __init__ import *

data = sc.textFile("%s/data/female/2016031418"  %(HDFS_OUTPUT_PATH) )

model = MatrixFactorizationModel.load(sc,"%s/model/als_female_parameters/30/als_female_0308-13_003"  %(HDFS_HOME_PATH))

data2predict=data.map(lambda x:json.loads(x)).filter(lambda x: x[0][0] and x[0][1])\
    .map(lambda x:((int(x[0][0]), int(x[0][1])),x[1]))
data2predict.cache()

#female1=data2predict.map(lambda x:(x[0][0],1)).reduceByKey(lambda x,y:x).count()

#female1 = data2predict.map(lambda x:(x,1)).reduceByKey(lambda x,y:x).count()

#predict the results
prediction = model.predictAll(data2predict.map(lambda x:x[0])).map(lambda x:((x.user, x.product), x.rating))


#female2 = prediction.reduceByKey(lambda x,y:x).count()

#female2= prediction.count()

#combining with the real results
combins = data2predict.join(prediction).map(lambda x:(x,1))\
        .reduceByKey(lambda x,y:x).map(lambda x: json.dumps(x[0]))

combins.saveAsTextFile("%s/results/parameters/female/30/2016031418_003_6days"  %(HDFS_OUTPUT_PATH) )

sc.stop()
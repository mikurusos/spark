from __init__ import *

totalCount=sc.accumulator(0)
invalidCount=sc.accumulator(0)

data = sc.textFile("%s/data/female/2016031218"  %(HDFS_OUTPUT_PATH) )

model = MatrixFactorizationModel.load(sc,"%s/model/als_female_0308-11"  %(HDFS_HOME_PATH))

data2predict=data.map(lambda x:json.loads(x)).filter(lambda x: x[0][0] and x[0][1])\
    .map(lambda x:((int(x[0][0]), int(x[0][1])),x[1]))
data2predict.cache()

#predict the results
prediction = model.predictAll(data2predict.map(lambda x:x[0])).map(lambda x:((x.user, x.product), x.rating))

# combining with the real results
combins = data2predict.join(prediction).map(lambda x: json.dumps(x))

combins.saveAsTextFile("%s/results/female/2016031218"  %(HDFS_OUTPUT_PATH) )
sc.stop()
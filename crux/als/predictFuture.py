from __init__ import *

totalCount=sc.accumulator(0)
invalidCount=sc.accumulator(0)

data = sc.textFile("hdfs://antispam/user/hadoop/output/chencheng/crux/data/rawData/2016030718-24/")

model = MatrixFactorizationModel.load(sc,"hdfs://antispam/user/hadoop/output/chencheng/model/als_female_2-7")

data2predict=data.map(lambda x:json.loads(x)).filter(lambda x: x[0][0] and x[0][1] and x[1])\
    .map(lambda x:((int(x[0][0]), int(x[0][1])),x[1]))
data2predict.cache()

#predict the results
prediction = model.predictAll(data2predict.map(lambda x:x[0])).map(lambda x:((x.user, x.product), x.rating))

# combining with the real results
combins = data2predict.join(prediction).map(lambda x: json.dumps(x))

combins.saveAsTextFile("hdfs://antispam/user/hadoop/output/chencheng/crux/results/2016030718")
sc.stop()
import util
from __init__ import *

data = sc.textFile("%s/data/testSample/female/20160315-1618"  %(HDFS_OUTPUT_PATH) )

model = MatrixFactorizationModel.load(sc,"%s/model/als_female_parameters/30/als_female_0311-14_30_003"  %(HDFS_HOME_PATH))

data2predict=util.prepareData2predict(data)
data2predict.cache()

#predict the results
prediction = util.predictData(model,data2predict)

#combining with the real results
combins = data2predict.join(prediction).map(lambda x:(x,1))\
        .reduceByKey(lambda x,y:x).map(lambda x: json.dumps(x[0]))

combins.saveAsTextFile("%s/data/testSample/female/20160315-1618_results_als_female_0311-14_30_003"  %(HDFS_OUTPUT_PATH) )

sc.stop()
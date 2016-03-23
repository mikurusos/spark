import util
from __init__ import *

data = sc.textFile("%s/data/male/2016032118"  %(HDFS_OUTPUT_PATH) )

model = MatrixFactorizationModel.load(sc,"%s/model/als_male_parameters/30/als_male_0317-21_003"  %(HDFS_HOME_PATH))

data2predict=util.prepareData2predict(data)
data2predict.cache()

#predict the results
prediction = util.predictData(model,data2predict)

#combining with the real results
combins = data2predict.join(prediction).distinct().map(lambda x: json.dumps(x))

combins.saveAsTextFile("%s/results/male/2016032118__0317-21_003"  %(HDFS_OUTPUT_PATH) )

sc.stop()
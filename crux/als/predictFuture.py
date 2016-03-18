import util
from __init__ import *

data = sc.textFile("%s/data/female/2016031418"  %(HDFS_OUTPUT_PATH) )

model = MatrixFactorizationModel.load(sc,"%s/model/als_male_parameters/30/als_female_0311-14_003"  %(HDFS_HOME_PATH))

data2predict=util.prepareData2predict(data)
data2predict.cache()

#predict the results
prediction = util.predictData(model,data2predict)

#combining with the real results
combins = data2predict.join(prediction).distinct().map(lambda x: json.dumps(x))

combins.saveAsTextFile("%s/results/female/2016031418"  %(HDFS_OUTPUT_PATH) )

sc.stop()
from __init__ import *

model = MatrixFactorizationModel.load(sc,"%s/model/als_male_parameters/25/als_male_0310-22_003"  %(HDFS_HOME_PATH))

male= model.userFeatures().map(lambda x:(x[0],list(x[1]))).map(lambda x:json.dumps(x))

male.saveAsTextFile("%s/data/realdata/0322_male/model_0310-22_003_male"  %(HDFS_OUTPUT_PATH) )

female= model.productFeatures().map(lambda x:(x[0],list(x[1]))).map(lambda x:json.dumps(x))

female.saveAsTextFile("%s/data/realdata/0322_male/model_0310-22_003_female"  %(HDFS_OUTPUT_PATH) )

sc.stop()


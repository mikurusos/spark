from __init__ import *

model = MatrixFactorizationModel.load(sc,"%s/model/als_female_parameters/30/als_female_0310-21_003"  %(HDFS_HOME_PATH))

female= model.userFeatures().map(lambda x:(x[0],list(x[1]))).map(lambda x:json.dumps(x))

female.saveAsTextFile("%s/data/realdata/0322/model_0310-21_003_female"  %(HDFS_OUTPUT_PATH) )

male= model.productFeatures().map(lambda x:(x[0],list(x[1]))).map(lambda x:json.dumps(x))

male.saveAsTextFile("%s/data/realdata/0322/model_0310-21_003_male"  %(HDFS_OUTPUT_PATH) )


sc.stop()


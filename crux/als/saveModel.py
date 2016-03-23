from __init__ import *

model = MatrixFactorizationModel.load(sc,"%s/model/als_female_parameters/30/als_female_0310-21_003"  %(HDFS_HOME_PATH))

female= model.userFeatures().map(lambda x:(x[0],list(x[1])))

female.saveAsTextFile("%s/data/realdata/0322/chengdu_model_female"  %(HDFS_OUTPUT_PATH) )

male= model.productFeatures().map(lambda x:(x[0],list(x[1])))

male.saveAsTextFile("%s/data/realdata/0322/chengdu_model_male"  %(HDFS_OUTPUT_PATH) )


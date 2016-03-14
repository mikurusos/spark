from __init__ import *
from computeAUC import *

totalCount=sc.accumulator(0)
invalidCount=sc.accumulator(0)

data = sc.textFile("hdfs://antispam/user/hadoop/output/chencheng/crux/results/2016030618/")

model = MatrixFactorizationModel.load(sc,"hdfs://antispam/user/hadoop/output/chencheng/model/als_female_2-7")

data.map(lambda x:json.loads(x))
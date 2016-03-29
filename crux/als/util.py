from __init__ import *

def prepareData2predict(rawData):
    return rawData.map(lambda x:json.loads(x)).filter(lambda x: x[0][0] and x[0][1])\
    .map(lambda x:((int(x[0][0]), int(x[0][1])),x[1]))


def predictData(model, data2predict):
    return model.predictAll(data2predict.map(lambda x:x[0])).map(lambda x:((x.user, x.product), x.rating))
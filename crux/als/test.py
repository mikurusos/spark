from __init__ import *

def getRawData(x):
    momoid = x["momoid"]
    api= x['server']["REQUEST_URI"].split('?')[0]
    return momoid, api

def combineDict(x,y):
    for item in y:
        if(item in x):
            x[item]+=y[item]
        else:
            x[item] = y[item]
    return x

data=sc.textFile("hdfs://antispam/user/flume/events/api/api_logs/2016/01/01/20/api_sink*")


data=data.map(lambda x: json.loads(x)).filter(lambda x:x["momoid"])\
    .map(getRawData).map(lambda x: (x[0], ( {x[1]:1}, 1) ))\
    .reduceByKey(combineDict).map(lambda x:json.dumps(x))


data.saveAsTextFile("hdfs://antispam/user/hadoop/output/chencheng/test")
sc.stop()


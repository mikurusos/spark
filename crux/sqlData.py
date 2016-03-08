import sys
import json
reload(sys)
sys.setdefaultencoding('utf-8')

from config import sc

from pyspark.sql import SQLContext, Row
sqlContext = SQLContext(sc)

data = sc.textFile("hdfs://antispam/user/hadoop/output/chencheng/crux/data/rawData/%s-24/"%(sys.argv[1]))

out = data.map(lambda x:json.loads(x)).filter(lambda x: x[0][0] and x[0][1] and x[1])\
        .map(lambda x: Row(sender=int(x[0][0]), receivor=int(x[0][1]), like=int(x[1])))

schema = sqlContext.createDataFrame(out)
schema.registerTempTable("people")

schema.write.save("hdfs://antispam/user/hadoop/output/chencheng/crux/data/dataFrame/%s/"%(sys.argv[1]))
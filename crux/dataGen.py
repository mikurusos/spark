# -*- coding: utf-8 -*-
import sys
import json
reload(sys)
sys.setdefaultencoding('utf-8')

from config import sc

data = sc.textFile("hdfs://antispam/user/hadoop/output/wang.yuqi/Venus/like_person/%s-24/" %(sys.argv[1]))

out = data.map(lambda x : x.split('\t')).map(lambda x: [json.loads(json.loads(x[0])), json.loads(x[1])[0]])\
        .map(lambda x: json.dumps(x))

out.saveAsTextFile("hdfs://antispam/user/hadoop/output/chencheng/crux/data/rawData/%s-24/"%(sys.argv[1]))
sc.stop()
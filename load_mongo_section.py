#-*- coding: utf-8 -*-
'''
https://blog.csdn.net/weixin_38569817/article/details/73478064
https://www.ibm.com/developerworks/cn/opensource/os-cn-python-yield/
https://blog.csdn.net/qq_34802511/article/details/82383409
'''

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import datetime
import pymongo
from bson.son import SON
import os

def get_Aday_Items(db, coll_name, date):
    Items = db[coll_name].find({"date": {'$eq': date}})
    return Items

def loadInBatches():
    client = pymongo.MongoClient('localhost', 27017)
    db = client["VnTrader_Tick_Db"]
    pipeline = [{"$group": {"_id": "$date", "count": {"$sum": 1}}},
                {"$sort": SON([("date", -1), ("_id", -1)])}]
    # item_all = get_Items(db, 'rb1901')
    coll = db['rb1901']
    dates = list(coll.aggregate(pipeline))
    print dates
    # startDate = dates[-1]["_id"]
    # endDate = dates[0]["_id"]
    # print pd.date_range(startDate, endDate, freq='D')[0]
    dateRange = [i["_id"] for i in dates]
    # strWhere = "this.date >=" + '\'' + startDate + '\'' + "&& this.date <" + '\'' + endDate + '\''
    # print dateRange

    for index, date in enumerate(dateRange):
        while i < len(dateRange):
            strSDate = str(dateRange[index])
            strEDate = str(dateRange[index + 1])
            print strSDate + strEDate
            strWhere = "this.date >=" + '\'' + startDate + '\'' + "&& this.date <" + '\'' + endDate + '\''
    # rb1901 = coll.find({"$where": strWhere})
    # path = strSDate[:7] + '.txt'
    # fp = open(path, 'w+')
    # for x in rb1901:
    #     x = [x for x in x.values() if type(x) == type('abc')]
    #     x = ','.join(x)
    #     fp.write(x)
    #     fp.write('\n')
    #     fp.flush()
    # fp.close()


loadInBatches()
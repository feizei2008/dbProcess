#-*- coding: utf-8 -*-

from pymongo import MongoClient
import pandas as pd
import time, datetime
import json
import logging
import os
from bson.son import SON

rawdata = ['rb1901', 'm1901', 'l1901', 'SR901', 'p1901']

def get_db(host, port, dbName):
    # 建立连接，似乎同一个主机上port只能唯一
    client = MongoClient(host, port)
    db = client[dbName]
    return db

def get_Oneday_Items(db, coll_name, date):
    Items = db[coll_name].find({"date": {'$eq': date}})  # 注：原始db的datetime为ISO格式需要转换才能比较
    return Items

db = get_db("localhost", 27017, 'VnTrader_Tick_Db')
dbNew = get_db("localhost", 27017, 'VnTrader_Tick_Db_Clean')

pipeline = [{"$group": {"_id": "$date", "count": {"$sum": 1}}},
            {"$sort": SON([("date", -1), ("_id", -1)])}]  # 按日期倒序排序，也可按count排，等等
dates = list(db['rb1901'].aggregate(pipeline))
dateRange = [k["_id"] for k in dates]
# print dateRange

dfraw = pd.DataFrame(list(get_Oneday_Items(db, 'rb1901', '20181019')))
dfclean = pd.DataFrame(list(get_Oneday_Items(dbNew, 'rb1901', '20181019')))
dfraw = dfraw.set_index(dfraw["_id"])
dfclean = dfclean.set_index(dfclean["_id"])
dfraw.to_csv("dfraw.csv")
dfclean.to_csv("dfclean.csv")
print len(dfraw)
print len(dfclean)
dfdiff = dfraw-dfclean
print len(dfdiff)
dfdiff.to_csv("dfdiff.csv")
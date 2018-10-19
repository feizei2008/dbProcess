#-*- coding: utf-8 -*-
'''
1、算出db.collections中每天有多少笔tick数据x；
2、算出该品种交易时间段的秒数*2=y；
3、x/y=z看是否在正常范围内
'''
from pymongo import MongoClient
from bson.son import SON
import pandas as pd
import time, datetime
import json
import logging
import os

timePoint = datetime.datetime.today() - datetime.timedelta(days=1)
timePoint = timePoint.replace(hour=21, minute=00, second=00, microsecond=0)

def get_db(host, port, dbName):
    # 建立连接，似乎同一个主机上port只能唯一
    client = MongoClient(host, port)
    db = client[dbName]
    return db

tickdb = get_db('localhost', 27017, 'VnTrader_Tick_Db')

def get_all_colls(db):
    return [i for i in db.collection_names()]

colls = get_all_colls(tickdb)
# print colls

def aggregate_by_date(db,coll_name):
    pipeline = [#{"$unwind": "$date"},unwind针对数组，此例中不需要
                {"$group": {"_id": "$date", "count": {"$sum": 1}}},
                {"$sort": SON([("date", -1), ("_id", -1)])}] # 按日期倒序排序，也可按count排，等等
    nums = list(db[coll_name].aggregate(pipeline)) # 这里db.coll写法会导致无法传入coll_name参数，必须是db[coll]写法
    # nums功能大致等同于python代码：VnTrader_Tick_db.rb1901.aggregate([{"$group": {"_id": "$date", "count": {"$sum": 1}}}])
    # mongo shell下查询指令：db.rb1901.aggregate([{$group : {_id : "$date", num_tutorial : {$sum : 1}}}])
    dfdate = pd.DataFrame(nums, columns=["count", "_id"])
    dfdate["Symbol"] = pd.Series(filter(str.isalpha,coll_name.lower()), index=range(len(dfdate)))
    dfdate = dfdate.set_index(dfdate["_id"])
    return dfdate
#
rbdate = aggregate_by_date(tickdb,'rb1901')#.Symbol[0]#[0]['_id']
# print rbdate

def get_list_index(tar,list):
    list.append(tar)
    list.sort()
    return list.index(tar)
# print get_list_index(6,[1,2,3,7,9])

def locate_trading_period(dfdate):
    dfInfo = pd.read_csv(os.getcwd() + '/BasicInformation.csv')
    dfInfo.index = dfInfo['Symbol'].tolist()
    del dfInfo['Symbol']
    collTradingPeriod = dfInfo.loc[dfdate.Symbol[0],"TradingPeriod"]
    # print collTradingPeriod
    # temp = collTradingPeriod.split('%')
    # temp1 = [i.split("||") for i in temp]
    # temp2 = [float(i[0].replace("-","")) for i in temp1]
    # temp3 = [i[1] for i in temp1]
    currPeriod = []
    for j in dfdate['_id']:#.tolist():
        # print j
        if '%' in collTradingPeriod:
            phase = [i for i in collTradingPeriod.split('%')]
            phase.sort(reverse=True) # 倒序排列
            # print phase
            for i in phase:
                startDate = float(i.split('||')[0].replace('-',''))
                if startDate <= float(j):
                    currPeriod.append(i.split('||')[1])
                else:
                    continue
        else:
            currPeriod.append(collTradingPeriod.split('||')[1])
    print currPeriod
    # dfdate["currentTradingPeriod"] = dfdate["_id"].apply(get_list_index())


locate_trading_period(rbdate)

# def loadInformation():
#     dfInfo = pd.read_csv(os.getcwd() + '/BasicInformation.csv')
#     dfInfo.index = dfInfo['Symbol'].tolist()
#     del dfInfo['Symbol']
#     # 增加对历史周期交易时间段变更的记录
#     dfInfo["CurrPeriod"] = dfInfo["TradingPeriod"].map(identifyCurrentPeriod)
#     # "TradingPeriod" column涵盖了品种在不同时间点起的日内交易时间段的变更（以螺纹为例，不一定准），需要一个map判断
#     return dfInfo

# def identifyCurrentPeriod(target):
#     if '%' in target:
#         phase = [i for i in target.split('%')]
#         phase.sort(reverse=False) # 倒序排列
#         for i in phase:
#             startDate = float(i.split('||')[0].replace('-',''))
#             if startDate <= timePoint:
#                 return i.split('||')[1]
#             else:
#                 continue
#     else:
#         return target.split('||')[1]

# def StandardizeTimePeriod(self,target):
#     tar = str(target)
#     # target参数应为原始数据中的time值
#     ms = 0
#     try:
#         tp = self.dfInfo.loc[self.Symbol]["CurrPeriod"]
#         time1 = [t for i in tp.split(',') for t in i.split('-')]
#         # time1是一个形如 ['8:59 ', ' 10:15', ' 10:30 ', ' 11:30', ' 13:30 ', ' 15:00']的列表
#         if '.' in tar:
#             ms = tar.split('.')[1]
#             # 形如vnpy的tick数据中time格式"09:10:16.5",ms为毫秒数
#             tar = tar.split('.')[0]
#
#         tar = time.strptime(tar, '%H:%M:%S')
#         for i in zip(*([iter(time1)] * 2)):
#             # zip形如[('8:59 ', ' 10:15'), (' 10:30 ', ' 11:30'), (' 13:30 ', ' 15:00')]
#             start = time.strptime(str(i[0]).strip(), '%H:%M')
#             end = time.strptime(str(i[1]).strip(), '%H:%M')
#             if self.compare_time(start,end,tar,ms):
#                 return True
#
#     except Exception, e:
#         print e

# def theoretical_tick_num(coll):


# now = datetime.datetime.now()
# hourago = now - datetime.timedelta(hours=0.85)
# timespan = now - hourago
# timespan.total_seconds()
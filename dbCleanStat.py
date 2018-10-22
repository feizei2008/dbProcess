#-*- coding: utf-8 -*-
'''
1、算出db.collections中每天有多少笔tick数据x；
2、算出该品种交易时间段的秒数*2=y；
3、x/y=z看是否在正常范围内
'''
from pymongo import MongoClient
from bson.son import SON
import pandas as pd
import datetime as dt
import os

rawdata = ['rb1901', 'm1901', 'l1901', 'SR901', 'p1901']

def get_db(host, port, dbName):
    # 建立连接，似乎同一个主机上port只能唯一
    client = MongoClient(host, port)
    db = client[dbName]
    return db

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

def get_list_index(tar,list):
    list.append(float(tar))
    list.sort()
    return list.index(tar)

def locate_trading_period(dfdate):
    dfInfo = pd.read_csv(os.getcwd() + '/BasicInformation.csv')
    dfInfo.index = dfInfo['Symbol'].tolist()
    del dfInfo['Symbol']
    tradingPeriods = dfInfo.loc[dfdate.Symbol[0], "TradingPeriod"]
    temp = tradingPeriods.split('%')
    temp1 = [i.split("||") for i in temp]
    validDates = [float(i[0].replace("-","")) for i in temp1]
    dayTimes = [i[1] for i in temp1]
    currPeriod = []
    for j in dfdate['_id']:
        i = get_list_index(float(j), validDates)
        currPeriod.append(dayTimes[i-1])
    dfdate['currPeriod'] = pd.Series(currPeriod, index=dfdate.index)
    return dfdate
    # for j in dfdate['_id']:#.tolist():
    #     if '%' in collTradingPeriod:
    #         phase = [i for i in collTradingPeriod.split('%')]
    #         phase.sort(reverse=False)
            # temp1 = [i.split("||") for i in phase]
            # temp2 = [float(i[0].replace("-", "")) for i in temp1]
            # temp3 = [i[1] for i in temp1]
            # temp2.append(float(j))
            # temp2.sort()
            # k = temp2.index(float(j))
            # currPeriod.append(temp3[k])
            # temp2.remove(float(j))
            # print temp2
            # for i in phase:
            #     startDate = float(i.split('||')[0].replace('-',''))
            #     if startDate <= float(j):
            #         currPeriod.append(i.split('||')[1])
            #     else:
            #         continue
        # else:
        #     currPeriod.append(collTradingPeriod.split('||')[1])

def theoretical_tick_num(current):
    current = "".join(current.split()) # 去除字符串内部空格
    ls1 = current.split(",")
    ls2 = [i.split("-") for i in ls1] # 子母list
    str2dt = lambda x: dt.datetime.strptime(x, "%H:%M") # 字符串"9:00"改为datetime时:分格式datetime.datetime(1900, 1, 1, 9, 0)
    ls3 = [map(str2dt, i) for i in ls2]
    ls4 = [(i[1] - i[0]).total_seconds() for i in ls3] # 连续交易时间段内秒数
    ls5 = [i + 86340 if i < 0 else i for i in ls4]
    """
    给负数加86340秒针对夜盘跨夜过零点的品种中20:59-00:00的时段，原始数据end(00:00)-start(20:59)=-75540；
    一天24小时有86400秒，00:00-20:59有75540秒，20:59-23:59有10800秒，23:59到零点整有60秒；
    集合竞价开始于20:59:00，结束于21:00:00，此一分钟内只在首尾各生成两笔tick数据，因此零点前的实际秒数可约等同end(23:59)-start(20:59)=10800秒，
    end(00:00)-start(20:59) + 86340 = -75540 + 86340 = 10800
    """
    return sum(ls5) * 2

def cmp(dfdate):
    dfdate['theoreticalTickNum'] = dfdate['currPeriod'].map(theoretical_tick_num)
    dfdate['count/theoretical'] = dfdate['count'] / dfdate['theoreticalTickNum']
    return dfdate

tickdb = get_db('localhost', 27017, 'VnTrader_Tick_Db_Clean')
for i in rawdata:
    colldate = aggregate_by_date(tickdb, i)
    colldate = locate_trading_period(colldate)
    cmp(colldate).to_csv('%s_%s_cmp.csv' % (i, dt.datetime.today().strftime('%Y-%m-%d')))
# rbdate = aggregate_by_date(tickdb, 'rb1901')#.Symbol[0]#[0]['_id']
# ldate = aggregate_by_date(tickdb, 'l1901')
# locate_trading_period(rbdate)
# cmp(rbdate).to_csv('cmp.csv')
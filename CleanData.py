#-*- coding: utf-8 -*-
'''
数据清洗
'''

from pymongo import MongoClient
import pandas as pd
import time

class CleanData(object):

    def __init__(self):

        self.dfInfo = self.loadInformation()
        self.removeList = []
        self.updateList = []
        self.logList = []
        # self.initCleanRegulation()

    def initCleanRegulation(self):
        db = self.get_db("localhost", 27017, 'MTS_TICK_DB')
        names = self.get_all_colls(db)
        for i in names:
            self.Symbol = filter(str.isalpha, str(i))
            self.df = pd.DataFrame(list(self.get_items(db, i)))
            self.cleanIllegalTradingTime()


    def get_db(self,host,port,dbName):
        #建立连接
        client = MongoClient(host,port)
        db = client[dbName]
        return db

    def get_all_colls(self,db):
        return [i for i in db.collection_names()]

    def get_items(self,db,coll_name):
        Items = db[coll_name].find()
        return Items

    def loadInformation(self):
        dfInfo = pd.read_csv('E:\dbProcess\\BasicInformation.csv')
        dfInfo.index = dfInfo['Symbol'].tolist()
        del dfInfo['Symbol']
        return dfInfo

    def cleanIllegalTradingTime(self):
        """删除非交易时段数据"""
        self.df['illegalTime'] = self.df["time"].map(self.StandardizeTimePeriod)
        if any(self.df[self.df['illegalTime'] == True]._values):
            self.removeList.extend(self.df[self.df['illegalTime'] == True]['_id'])
        del self.df["illegalTime"]

    def cleanSameTimestamp(self):
        """清除重复时间戳，记录"""
        idList = self.df[self.df["datetime"].duplicated()]["_id"]
        for i in idList._values:
            self.removeList.append(i)

    def cleanNullVolTurn(self):
        """Tick有成交，但volume和turnover为0"""
        f = lambda x: float(x)
        self.df["lastVolume"].map(f)
        self.df["lastTurnover"].map(f)
        self.df["volume"].map(f)
        self.df["turnover"].map(f)
        self.df["openInterest"].map(f)
        self.df["lastPrice"].map(f)

        lastVol = self.df["lastVolume"] != 0.0
        lastTurn = self.df["lastTurnover"] != 0.0
        Vol = self.df["volume"] == 0.0
        Turn = self.df["turnover"] == 0.0
        openIn = self.df["openInterest"] == 0.0
        lastP = self.df["lastPrice"] != 0.0

        # lastTurn为0,lastVolume和lastPrice不为0
        dfTemp = self.df.loc[~lastTurn & lastVol & lastP]
        dfTemp["lastTurnover"] = dfTemp["lastVolume"] * dfTemp["lastPrice"]
        for i, row in dfTemp.iterrows():
            self.updateList.append(row)

        # lastVolume为0,lastTurnover和lastPrice不为0
        dfTemp = self.df.loc[lastTurn & ~lastVol & lastP]
        dfTemp["lastVolume"] = dfTemp["lastTurnover"] / dfTemp["lastPrice"]
        for i, row in dfTemp.iterrows():
            self.updateList.append(row)

        # lastPrice为0,lastVolume和lastTurnover不为0
        dfTemp = self.df.loc[lastTurn & lastVol & ~lastP]
        dfTemp["lastPrice"] = dfTemp["lastTurnover"] / dfTemp["lastVolume"]
        for i, row in dfTemp.iterrows():
            self.updateList.append(row)

        # lastVolume和lastTurnover均不为0
        dfTemp = self.df.loc[lastVol & lastTurn & (Vol | Turn | openIn)]

        # volume、openInterest、turnover均为0，删除并记录
        if dfTemp.loc[Vol & Turn & openIn]._values.any():
            self.removeList.extend(i for i in dfTemp.loc[Vol & Turn & openIn]["_id"]._values)
            self.logList.extend(i for i in dfTemp.loc[Vol & Turn & openIn]["_id"]._values)

        # turnover为0,lastVol不为0
        for i, row in self.df[Turn & lastVol].iterrows():
            preIndex = i - 1
            if preIndex >= 0:
                row["turnover"] = self.df.iloc[preIndex]["turnover"] + row["lastTurnover"]
                self.updateList.append(row)

        # volume为0,lastVol不为0
        for i,row in self.df[Vol & lastVol].iterrows():
            preIndex = i - 1
            if preIndex >= 0:
                row["volume"] = self.df.iloc[preIndex]["volume"] + row["lastVolume"]
                self.updateList.append(row)

    def cleanNullOpenInter(self):
        """持仓量为0,用上一个填充"""
        self.paddingWithPrevious("openInterest")

    def cleanNullPriceIndicator(self):
        lastP = self.df["lastPrice"] == 0.0
        high = self.df["highPrice"] == 0.0
        low = self.df["lowPrice"] == 0.0
        bidP = self.df["bidPrice1"] == 0.0
        askP = self.df["askPrice1"] == 0.0
        #如果均为0，删除
        if self.df.loc[lastP & high & low & bidP & askP]._values.any():
            self.removeList.extend(i for i in self.df.loc[lastP & high & low & bidP & askP]["_id"]._values)

        if self.df.loc[lastP]["_id"] not in self.removeList:
            self.paddingWithPrevious("lastPrice")
        if self.df.loc[high]["_id"] not in self.removeList:
            self.paddingWithPrevious("highPrice")
        if self.df.loc[low]["_id"] not in self.removeList:
            self.paddingWithPrevious("lowPrice")
        if self.df.loc[bidP]["_id"] not in self.removeList:
            self.paddingWithPrevious("bidPrice1")
        if self.df.loc[askP]["_id"] not in self.removeList:
            self.paddingWithPrevious("askPrice1")

    def recordExceptionalPrice(self):
        self.estimateExceptional("lastPrice")
        self.estimateExceptional("highPrice")
        self.estimateExceptional("lowPrice")
        self.estimateExceptional("bidPrice1")
        self.estimateExceptional("askPrice1")

    def estimateExceptional(self,field):
        dfTemp = self.df[field]
        dfTemp["delta"] = self.df[field] - self.df[field].shift(1)
        dfTemp["IsExcept"] = dfTemp["delta"] >= dfTemp[field].shift(1) * 0.05
        if any(dfTemp.loc[dfTemp["IsExcept"]]._values):
            self.logList.extend(dfTemp.loc[dfTemp["IsExcept"],["_id"]])

    def paddingWithPrevious(self,field):
        for i, row in self.df.loc[self.df[field] == 0.0].iterrows():
            preIndex = i - 1
            if preIndex >= 0:
                row[field] = self.df.iloc[preIndex][field]
                self.updateList.append(row)

    def StandardizeTimePeriod(self,target):
        tar = target
        ms = 0
        tp = self.dfInfo.loc[self.dfInfo["Symbol"] == self.Symbol]["TradingPeriod"]
        time1 = [t for i in tp[0].split(',') for t in i.split('-')]
        if '.' in tar:
            tar = tar.split('.')[0]
            ms = tar.split('.')[1]
        tar = time.strptime(tar, '%H:%M:%S')
        for i in zip(*([iter(time1)] * 2)):
            start = time.strptime(str(i[0]).strip(), '%H:%M')
            end = time.strptime(str(i[1]).strip(), '%H:%M')
            if self.compare_time(start,end,tar,ms):
                return True

        return False

    def compare_time(self,s1,s2,st,ms):
        """由于time类型没有millisecond，故单取ms进行逻辑判断"""
        if st > s1 and st < s2:
            return True
        elif (st == s1 and ms == 0) or (st == s2 and ms == 0):
            return True
        else:
            return False

if __name__ == "__main__":
    CleanData()
    # dfInfo = loadInformation()
    # db = get_db("localhost", 27017, 'MTS_TICK_DB')
    # names = get_all_colls(db)
    # for i in names:
    #     Symbol = filter(str.isalpha, i)
    #     df = pd.DataFrame(list(get_items(db, i)))
    #     pass

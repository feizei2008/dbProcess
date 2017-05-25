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
        self.initCleanRegulation()

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
        self.removeList.extend(self.df[self.df['illegalTime'] == True]['_id'])
        del self.df["illegalTime"]

    def cleanSameTimestamp(self):
        """清除重复时间戳，记录"""
        self.removeList.append(self.df[self.df.duplicated["datetime"] == True]["_id"])

    def cleanNullVolTurn(self):
        """Tick有成交，但volume和turnover为0"""
        lastVol = self.df['lastVolume'] != 0
        lastTurn = self.df["lastTurnover"] != 0
        Vol = self.df["volume"] == 0
        Turn = self.df["turnover"] == 0
        openIn = self.df["openInterest"] == 0
        lastP = self.df["lastPrice"] != 0

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
        self.removeList.extend(dfTemp.loc[Vol & Turn & openIn,["_id"]])

        # turnover为0,lastVol不为0
        for i, row in dfTemp[Turn].iterrows():
            preIndex = row.index[0] - 1
            if preIndex >= 0:
                row["turnover"] = dfTemp.iloc[preIndex]["turnover"] + row["lastTurnover"]
                self.updateList.append(row)

        # volume为0,lastVolume不为0
        for i,row in dfTemp[Vol].iterrows():
            preIndex = row.index[0] - 1
            if preIndex >= 0:
                row["volume"] = dfTemp.iloc[preIndex]["volume"] + row["lastVolume"]
                self.updateList.append(row)

    def cleanNullOpenInter(self):
        """持仓量为0,用上一个填充"""
        self.paddingWithPrevious("openInterest")

    def cleanNullPriceIndicator(self):
        lastP = self.df["lastPrice"] == 0
        high = self.df["highPrice"] == 0
        low = self.df["lowPrice"] == 0
        bidP = self.df["bidPrice1"] == 0
        askP = self.df["askPrice1"] == 0
        #如果均为0，删除
        self.removeList.extend(self.df.loc[lastP & high & low & bidP & askP, ["_id"]])

        if self.df.loc[lastP, ["_id"]] not in self.removeList:
            self.paddingWithPrevious("lastPrice")
        if self.df.loc[high, ["_id"]] not in self.removeList:
            self.paddingWithPrevious("highPrice")
        if self.df.loc[low, ["_id"]] not in self.removeList:
            self.paddingWithPrevious("lowPrice")
        if self.df.loc[bidP, ["_id"]] not in self.removeList:
            self.paddingWithPrevious("bidPrice1")
        if self.df.loc[askP, ["_id"]] not in self.removeList:
            self.paddingWithPrevious("askPrice1")

    def recordExceptionalPrice(self):
        dfTemp = self.df["lastPrice"]
        dfTemp["delta"] = self.df["lastPrice"] - self.df["lastPrice"].shift(1)
        pass



    def paddingWithPrevious(self,field):
        dfTemp = self.df.loc[self.df[field] == 0]
        for i, row in dfTemp.iterrows():
            preIndex = row.index[0] - 1
            if preIndex >= 0:
                row[field] = self.df[preIndex][field]
                self.updateList.append(row)

    def StandardizeTimePeriod(self,target):
        ms = 0
        tp = self.dfInfo.ix[self.Symbol]['TradingPeriod']
        time1 = [t for i in tp.split(',') for t in i.split('-')]
        if '.' in target:
            target = target.split('.')[0]
            ms = target.split('.')[1]
        target = time.strptime(target, '%H:%M:%S')
        for i in zip(*([iter(time1)] * 2)):
            start = time.strptime(str(i[0]).strip(), '%H:%M')
            end = time.strptime(str(i[1]).strip(), '%H:%M')
            if self.compare_time(start,end,target,ms):
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

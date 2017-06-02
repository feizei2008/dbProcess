#-*- coding: utf-8 -*-
'''
K线聚合
'''

import pandas as pd
import datetime
import json
from CleanData import CleanData

class AggregateKData(object):

    def __init__(self):
        self.cleanData = CleanData()

    def initStart(self):
        # db = self.cleanData.get_db("localhost", 27017, 'test_MTS_TICK_DB')
        db = self.getDB("TICK")
        names = self.cleanData.get_all_colls(db)
        for i in names:
            self.df = pd.DataFrame(list(self.cleanData.get_specificItems(db, i, self.cleanData.timePoint)))
            if not self.df.empty:
                pass

    def processOneMin(self, coll_name):
        cycle = "1MIN"
        start = self.df.loc[0,"datetime"]
        end = start + datetime.timedelta(seconds=60)
        dfTemp = self.df.loc[self.df["datetime"] >= start & self.df["datetime"] <= end]
        df = dfTemp.apply(sum)
        maxTime = dfTemp["datetime"].argmax()
        df["datetime"] = dfTemp.loc[maxTime, "datetime"]
        df["time"] = dfTemp.loc[maxTime, "time"]
        df["date"] = dfTemp.loc[maxTime, "date"]

        self.insert2bar(df,self.getDB(cycle),coll_name)

    def getDB(self, cycle):
        cycle = cycle
        dbNew = self.cleanData.get_db("localhost", 27017, 'test_MTS_' + cycle + '_DB')
        return dbNew

    def insert2bar(self, rawData, dbNew, coll_name):
        del rawData["_id"]
        kData = json.loads(rawData.T.to_json()).values()
        dbNew[coll_name].insert_one(kData)





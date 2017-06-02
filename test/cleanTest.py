#encoding: utf-8

import unittest
import pandas as pd
from CleanData import CleanData
import time



class myTest(unittest.TestCase):

    def setUp(self):
        self.CD = CleanData()
        db = self.CD.get_db("localhost", 27017, 'MTS_TICK_DB')
        i = 'CFTest'
        self.CD.Symbol = 'cf'
        self.CD.timePoint = time.strptime("2017-05-01", '%Y-%m-%d')
        self.CD.df = pd.DataFrame(list(self.CD.get_specificItems(db, i, self.CD.timePoint)))
        self.CD.dfInfo = pd.read_csv("E:\\dbProcess\\test\\test.csv")


    def test_illegalTradingTime(self):
        self.CD.cleanIllegalTradingTime()
        removeList = self.CD.removeList
        self.assertIn(0, removeList)
        self.assertIn(2, removeList)
        self.assertIn(3, removeList)

    def test_cleanSameTimestamp(self):
        self.CD.cleanSameTimestamp()
        testList = self.CD.removeList
        self.assertIn(9, testList)

    def test_cleanNullVolTurn(self):
        self.CD.cleanNullVolTurn()
        df = self.CD.df
        updateList = self.CD.updateList
        removeList = self.CD.removeList
        for i in updateList:
            if i == 0:
                self.assertEqual(df.loc[i,"lastTurnover"], 1000.0)
            if i == 1:
                self.assertEqual(df.loc[i,"lastVolume"], 2.0)
            if i == 2:
                self.assertEqual(df.loc[i,"lastPrice"], 10.0)
            # turnover为0,lastVol不为0
            if i == 4:
                self.assertEqual(df.loc[i,"turnover"], 120.0)
            # volume为0,lastVolume不为0
            if i == 5:
                self.assertEqual(df.loc[i,"volume"], 110.0)

        # volume、openInterest、turnover均为0
        self.assertIn(3, removeList)

    def test_cleanNullOpenInter(self):
        self.CD.cleanNullOpenInter()
        df = self.CD.df
        updateList = self.CD.updateList
        for i in updateList:
            if i == 6:
                self.assertEqual(df.loc[i,"openInterest"], 52772.0)

    def test_cleanNullPriceIndicator(self):
        self.CD.cleanNullPriceIndicator()
        removeList = self.CD.removeList
        self.assertIn(7, removeList)
        df = self.CD.df
        updateList = self.CD.updateList
        for i in updateList:
            if i == 9:
                self.assertEqual(df.loc[i,"lastPrice"], 10.0)
            if i == 10:
                self.assertEqual(df.loc[i,"highPrice"], 10.0)
            if i == 11:
                self.assertEqual(df.loc[i,"lowPrice"], 15660.0)
            if i == 12:
                self.assertEqual(df.loc[i,"bidPrice1"], 15645.0)
            if i == 13:
                self.assertEqual(df.loc[i,"askPrice1"], 15660.0)

    def test_recordExceptionalPrice(self):
        self.CD.recordExceptionalPrice()
        logList = self.CD.logList
        self.assertIn(9, logList)
        self.assertIn(10, logList)
        self.assertIn(11, logList)
        self.assertIn(12, logList)
        self.assertIn(13, logList)




if __name__ == "__main__":
    unittest.main()
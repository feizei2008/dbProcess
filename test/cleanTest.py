#encoding: utf-8

import unittest
import pandas as pd
from CleanData import CleanData

class myTest(unittest.TestCase):

    def setUp(self):
        self.CD = CleanData()
        db = self.CD.get_db("localhost", 27017, 'MTS_TICK_DB')
        i = 'CFTest'
        self.CD.Symbol = 'cf'
        self.CD.df = pd.DataFrame(list(self.CD.get_items(db, i)))
        self.CD.dfInfo = pd.read_csv("E:\\dbProcess\\test\\test.csv")


    # def test_illegalTradingTime(self):
    #     self.CD.cleanIllegalTradingTime()
    #     testList = self.CD.removeList
    #     self.assertEqual(len(testList), 4)


    def test_cleanSameTimestamp(self):
        self.CD.cleanSameTimestamp()
        testList = self.CD.removeList
        testList = [str(x) for x in testList]
        self.assertIn("590b25de7405ae0cb063b100", testList)

    def test_cleanNullVolTurn(self):
        self.CD.cleanNullVolTurn()
        updateList = self.CD.updateList
        removeList = self.CD.removeList
        removeList = [str(x) for x in removeList]
        for i in updateList:
            if "590b25de7405ae0cb063b087" == str(i["_id"]):
                self.assertEqual(i["lastTurnover"], 1000.0)
            if "590b25de7405ae0cb063b088" == str(i["_id"]):
                self.assertEqual(i["lastVolume"], 2.0)
            if "590b25de7405ae0cb063b089" == str(i["_id"]):
                self.assertEqual(i["lastPrice"], 10.0)
            # turnover为0,lastVol不为0
            if "590b25de7405ae0cb063b091" == str(i["_id"]):
                self.assertEqual(i["turnover"], 10.0)
            # volume为0,lastVolume不为0
            if "590b25de7405ae0cb063b092" == str(i["_id"]):
                self.assertEqual(i["volume"], 110.0)

        # volume、openInterest、turnover均为0
        self.assertIn("590b25de7405ae0cb063b090", removeList)

    def test_cleanNullOpenInter(self):
        self.CD.cleanNullOpenInter()
        updateList = self.CD.updateList
        for i in updateList:
            if "590b25de7405ae0cb063b095" == str(i["_id"]):
                self.assertEqual(i["openInterest"], 52772.0)

    def test_cleanNullPriceIndicator(self):
        self.CD.cleanNullPriceIndicator()
        removeList = self.CD.removeList
        self.assertIn("590b25de7405ae0cb063b097", removeList)

if __name__ == "__main__":
    unittest.main()
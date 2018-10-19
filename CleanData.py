#-*- coding: utf-8 -*-
'''
数据清洗
'''

from pymongo import MongoClient
import pandas as pd
import time, datetime
import json
import logging
import os

# class Logger:
#     def __init__(self, path, clevel=logging.DEBUG, Flevel=logging.DEBUG):
#         self.logger = logging.getLogger(path)
#         self.logger.setLevel(logging.DEBUG)
#         fmt = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S')
#         # 设置CMD日志
#         sh = logging.StreamHandler()
#         sh.setFormatter(fmt)
#         sh.setLevel(clevel)
#         # 设置文件日志
#         fh = logging.FileHandler(path)
#         fh.setFormatter(fmt)
#         fh.setLevel(Flevel)
#         self.logger.addHandler(sh)
#         self.logger.addHandler(fh)
#
#     def debug(self, message):
#         self.logger.debug(message)
#
#     def info(self, message):
#         self.logger.info(message)
#
#     def war(self, message, color=FOREGROUND_YELLOW):
#         set_color(color)
#         self.logger.warn(message)
#         set_color(FOREGROUND_WHITE)
#
#     def error(self, message, color=FOREGROUND_RED):
#         set_color(color)
#         self.logger.error(message)
#         set_color(FOREGROUND_WHITE)
#
#     def cri(self, message):
#         self.logger.critical(message)

LOG_FILE = os.getcwd() + '\\' + 'LogFile\\' + time.strftime('%Y-%m-%d',time.localtime(time.time()))  + ".log"
try:
    f =open(LOG_FILE,'r')
    f.close()
except IOError:
    f = open(LOG_FILE,'w')
    f.close()
logging.basicConfig(filename=LOG_FILE, level=logging.DEBUG)
logger = logging.getLogger(__name__)

def add_log(func):
    def newFunc(*args, **kwargs):
        logger.warning("Before %s() call on %s" % (func.__name__, time.strftime("%Y-%m-%d %H:%M:%S")))
        ret = func(*args, **kwargs)
        logger.warning("After %s() call on %s" % (func.__name__, time.strftime("%Y-%m-%d %H:%M:%S")))
        return ret
    return newFunc


class CleanData(object):

    def __init__(self):
        timePoint = datetime.datetime.today() - datetime.timedelta(days=1)
        self.timePoint = timePoint.replace(hour=21, minute=00, second=00, microsecond=0)
        # timePoint为夜盘的晚上21点整，品种理论交易日的开始
        self.dfInfo = self.loadInformation()
        self.AucTime = ['8:59:00', '20:59:00', '9:29:00', '9:14:00']
        # AuctionPrice集合竞价价格，参见：http://www.yhqh.net/html/33-40/40953.htm
        '''
        dce、shfe、czce的日盘集合竞价申报阶段为08:55—08:59，撮合阶段为08:59—09:00，在09:00:00生成AucPri；
        dce、shfe、czce的夜盘集合竞价申报阶段为20:55—20:59，撮合阶段为20:59—21:00，在21:00:00生成AucPri;
        中金所股指期货集合竞价时间为9:25—9:30，在09:29:00生成AucPri，09:30:00开始连续竞价；
        中金所国债期货集合竞价时间为9:10—9:15，在09:14:00生成AucPri，09:15:00开始连续竞价。
        '''

    def initList(self):
        self.removeList = []
        self.updateList = []
        self.logList = []

    def initCleanRegulation(self):
        # db = self.get_db("192.168.1.80", 27017, 'MTS_TICK_DB')
        db = self.get_db("localhost", 27017, 'VnTrader_Tick_Db')
        # db是原始未清洗的数据库
        # dbNew = self.get_db("localhost", 27017, 'test_MTS_TICK_DB')
        dbNew = self.get_db("localhost", 27017, 'VnTrader_Tick_Db_Clean')
        # dbNew是清洗过的新数据库，建立新db server失败先跳过，似乎同一个主机上port只能唯一（不知道为什么dbNew自己建立了）
        names = self.get_all_colls(db)
        for i in names:
            if 'sc' in i:
                # 根据csv文件，sc是指的shfe，但这是什么意思没懂
                continue
            if i in ['rb1901','m1901','l1901','SR901','p1901']:
                try:
                    print "start process collection %s........." %(i)
                    logger.warning("start process collection %s........." %(i))
                    self.Symbol = filter(str.isalpha, str(i)).lower()
                    # Symbol为db的collections名称中（如'SR809'）的字母部分改成小写（'sr'）,因为csv文件中都是小写，为了以后方便匹配
                    self.df = pd.DataFrame(list(self.get_specificItems(db, i, self.timePoint)))
                    # get_specificItems用于找出collection中大于等于某一时间点的数据，此处参数为T-1的21点整
                    self.initList()
                    if not self.df.empty:
                        self.cleanIllegalTradingTime()
                        self.reserveLastTickInAuc()
                        self.cleanSameTimestamp()
                        self.cleanExceptionalPrice()
                        # self.cleanNullVolTurn()
                        self.cleanNullPriceIndicator()
                        self.cleanNullOpenInter()
                        self.recordExceptionalPrice()

                        self.delItemsFromRemove()
                        # ？
                        self.insert2db(dbNew,i)
                        # 清洗后的数据插入到新的db中
                except Exception, e:
                    print e
                    logger.error(e)
                    continue

    def get_db(self,host,port,dbName):
        # 建立连接，似乎同一个主机上port只能唯一
        client = MongoClient(host,port)
        db = client[dbName]
        return db

    def get_all_colls(self,db):
        return [i for i in db.collection_names()]

    def get_specificItems(self, db, coll_name, time):
        Items = db[coll_name].find({"datetime": {'$gte': time}}) # 注：原始db的datetime为ISO格式需要转换才能比较
        return Items

    def insert2db(self,dbNew,coll_name):
        del self.df["_id"]
        self.df = self.df.dropna(axis=0, how='all') # all是整行数值都为na才删除，any是有一个为na就删除
        # 0101,行列行列
        data = json.loads(self.df.T.to_json(date_format = 'iso')).values() # 返回字典组成的列表，不过这步似乎多余，因datetime本来就是ISO格式
        # df.T是倒置transpose
        # isoformat() Return a string representing the date in ISO 8601 format, ‘YYYY-MM-DD’. For example, date(2002, 12, 4).isoformat() == '2002-12-04'.
        for i in data:
            if isinstance(i["datetime"], unicode):
                i["datetime"] = datetime.datetime.strptime(i["datetime"], "%Y-%m-%dT%H:%M:%S.%fZ")
        dbNew[coll_name].insert_many(data)

    def loadInformation(self):
        dfInfo = pd.read_csv(os.getcwd() + '/BasicInformation.csv')
        dfInfo.index = dfInfo['Symbol'].tolist()
        del dfInfo['Symbol']
        # 增加对历史周期交易时间段变更的记录
        dfInfo["CurrPeriod"] = dfInfo["TradingPeriod"].map(self.identifyCurrentPeriod)
        # "TradingPeriod" column涵盖了品种在不同时间点起的日内交易时间段的变更（以螺纹为例，不一定准），需要一个map判断
        return dfInfo

    def identifyCurrentPeriod(self, target):
        if '%' in target:
            phase = [i for i in target.split('%')]
            phase.sort(reverse=True)
            # 倒序一下，最新时间段放前面
            for i in phase:
                startDate = datetime.datetime.strptime(i.split('||')[0], "%Y-%m-%d")
                if startDate <= self.timePoint:
                    return i.split('||')[1]
                else:
                    continue
        else: # 没有%说明历史交易时间段只有一个
            return target.split('||')[1]

    @add_log
    def cleanIllegalTradingTime(self):
        """删除非交易时段数据"""
        self.df['illegalTime'] = self.df["time"].map(self.StandardizeTimePeriod) #map后交易时段内true，外false
        # vnpy 的tick数据的time值例如 09:10:16.0, 09:10:16.5，分别代表该秒内的2个tick
        # map的结果是给出true or false的结论，并未直接起到筛选作用
        self.df['illegalTime'] = self.df['illegalTime'].fillna(False)
        # inplace = False，代表就地修改(true)还是返回一个副本(false)，默认false
        # fillna 把nan值填充为0（为什么不是继承前值？），会被接下来的方法进一步洗掉
        for i,row in self.df[self.df['illegalTime'] == False].iterrows():
            self.removeList.append(i)
            # removeList中是被洗出去的非交易时间段内数据
            logger.info('remove index = %d, id = %s' %(i, row["_id"]))
        del self.df["illegalTime"]

    @add_log
    def reserveLastTickInAuc(self):
        """保留集合竞价期间最后一个tick数据"""
        self.df["structTime"] = self.df["time"].map(lambda x: datetime.datetime.strptime(x, "%H:%M:%S.%f"))
        for st in self.AucTime:
            start = datetime.datetime.strptime(st, '%H:%M:%S')
            end = start + datetime.timedelta(minutes=1)
            p1 = self.df["structTime"] >= start
            p2 = self.df["structTime"] < end # 注意这里小于号
            dfTemp = self.df.loc[p1 & p2]
            dfTemp = dfTemp.sort_values(by=["structTime"], ascending=False)
            for i in dfTemp.index.values[1:]:
                self.removeList.append(i)
                # removeList中加入集合竞价（连续竞价前1分钟内的）数据
                logger.info('remove index = %d' % i)

    @add_log
    def cleanSameTimestamp(self):
        """清除重复时间戳，记录"""
        dfTemp = self.df.sort_values(by = ['datetime'], ascending = False)
        idList = dfTemp[dfTemp["datetime"].duplicated()].index
        # duplicated方法默认参数对首次出现的重复值显示false，以后出现的才显示true
        for i in idList.values:
            self.removeList.append(i)
            # removeList中加入被剔除的重复时间戳数据
            logger.info('remove index = %d' % i)

    @add_log
    def cleanExceptionalPrice(self):
        """清理异常价格数据"""
        openP = self.df["openPrice"] >= 1e+308
        # 1e+308指1 * 10的308次方，原始ctp tick数据的无效数据形如：1.79769313486e+308
        highP = self.df["highPrice"] >= 1e+308
        # settleP = self.df["settlementPrice"] >= 1e+308
        lowP = self.df["lowPrice"] >= 1e+308

        dfTemp = self.df.loc[openP | highP | lowP]
        # 竖杠为or
        for i, row in dfTemp.iterrows():
            if i not in self.removeList:
                self.removeList.append(i)
                # removeList中加入异常价格数据（适用bar数据；tick数据根据抽查似乎未见那么大的异常值，但仍可有备无患）
                logger.info('remove index = %d, id = %s' % (i, row["_id"]))

    # @add_log
    # def cleanNullVolTurn(self):
    #     """Tick有成交，但volume和turnover为0"""
    #     # 注意tick数据中volume为当日累计成交量，且tick数据无turnover
    #     # tick数据中和量有关的只有openInterest（持仓量）和volume（当日累计成交量）
    #     # 这个函数在vnpy的数据清洗中应做修改
    #     f = lambda x: float(x)
    #     self.df["lastVolume"] = self.df["lastVolume"].map(f) # vnpy，tick此值始终为0，bar无此值
    #     self.df["lastTurnover"] = self.df["lastTurnover"].map(f) # vnpy，tick、bar均无此值
    #     self.df["volume"] = self.df["volume"].map(f) # vnpy，tick为当日累计成交量，bar为本bar内成交量
    #     self.df["turnover"] = self.df["turnover"].map(f) # vnpy，tick、bar均无此值
    #     self.df["openInterest"] = self.df["openInterest"].map(f) # 持仓量
    #     self.df["lastPrice"] = self.df["lastPrice"].map(f)
    #
    #     lastVol = self.df["lastVolume"] != 0.0 # lastVolume不为0时为true
    #     lastTurn = self.df["lastTurnover"] != 0.0 # lastTurnover不为0时为true
    #     Vol = self.df["volume"] == 0.0 # volume为全天的，为0时为true
    #     Turn = self.df["turnover"] == 0.0
    #     openIn = self.df["openInterest"] == 0.0
    #     lastP = self.df["lastPrice"] != 0.0
    #
    #     tu = self.dfInfo.loc[self.Symbol]["TradingUnits"] # 品种合约倍数
    #
    #     # lastTurn为0,lastVolume和lastPrice不为0
    #     dfTemp = self.df.loc[~lastTurn & lastVol & lastP] # 取反只对lastTurn起作用，三个条件为：False & True & True
    #     # ~：按位取反运算符：对数据的每个二进制位取反,即把1变为0,把0变为1 。~x 类似于 -x-1
    #     # eg：a = 0011 1100 = 60，~a = 1100 0011 = -61
    #     # True = 1，False = 0，对True取反为-2，对False取反为-1
    #     if not dfTemp.empty:
    #         dfTemp["lastTurnover"] = dfTemp["lastVolume"] * dfTemp["lastPrice"] * float(tu) # 成交合约价值
    #         for i, row in dfTemp.iterrows():
    #             if i not in self.removeList:
    #                 self.df.loc[i,"lastTurnover"] = row["lastTurnover"]
    #                 self.updateList.append(i)
    #                 logger.info('lastTurn = 0, update index = %d, id = %s' % (i, row["_id"]))
    #
    #     # lastVolume为0,lastTurnover和lastPrice不为0
    #     dfTemp = self.df.loc[lastTurn & ~lastVol & lastP]
    #     if not dfTemp.empty:
    #         dfTemp["lastVolume"] = dfTemp["lastTurnover"] / (dfTemp["lastPrice"] * float(tu))
    #         dfTemp["lastVolume"].map(lambda x:int(round(x)))
    #         for i, row in dfTemp.iterrows():
    #             if i not in self.removeList:
    #                 self.df.loc[i,"lastVolume"] = row["lastVolume"]
    #                 self.updateList.append(i)
    #                 logger.info('lastVol = 0, update index = %d, id = %s' % (i, row["_id"]))
    #
    #     # lastPrice为0,lastVolume和lastTurnover不为0
    #     dfTemp = self.df.loc[lastTurn & lastVol & ~lastP]
    #     if not dfTemp.empty:
    #         dfTemp["lastPrice"] = dfTemp["lastTurnover"] / (dfTemp["lastVolume"] * float(tu))
    #         for i, row in dfTemp.iterrows():
    #             if i not in self.removeList:
    #                 self.df.loc[i,"lastPrice"] = row["lastPrice"]
    #                 self.updateList.append(i)
    #                 logger.info('lastPrice = 0, update index = %d, id = %s' % (i, row["_id"]))
    #
    #     # lastVolume和lastTurnover均不为0，且vol，Turn，openIn任一为0
    #     dfTemp = self.df.loc[lastVol & lastTurn & (Vol | Turn | openIn)]
    #     if not dfTemp.empty:
    #         # volume、openInterest、turnover均为0，删除并记录
    #         if dfTemp.loc[Vol & Turn & openIn]._values.any():
    #             for i in dfTemp.loc[Vol & Turn & openIn].index.values:
    #                 if i not in self.removeList:
    #                     self.removeList.append(i)
    #                     self.logList.append(i)
    #                     logger.info('Vol & openInterest & turn = 0, remove index = %d' % i)
    #
    #         # turnover为0,lastVol不为0
    #         for i, row in self.df[Turn & lastVol].iterrows():
    #             preIndex = i - 1
    #             if preIndex >= 0 and i not in self.removeList:
    #                 row["turnover"] = self.df.loc[preIndex,"turnover"] + row["lastTurnover"]
    #                 self.df.loc[i,"turnover"] = row["turnover"]
    #                 self.updateList.append(i)
    #                 logger.info('Turn = 0 & lastTurn != 0, update index = %d, id = %s' % (i, row["_id"]))
    #
    #         # volume为0,lastVol不为0
    #         for i,row in self.df[Vol & lastVol].iterrows():
    #             preIndex = i - 1
    #             if preIndex >= 0 and i not in self.removeList:
    #                 row["volume"] = self.df.loc[preIndex,"volume"] + row["lastVolume"]
    #                 self.df.loc[i,"volume"] = row["volume"]
    #                 self.updateList.append(i)
    #                 logger.info('Vol = 0 & lastVol != 0, update index = %d, id = %s' % (i, row["_id"]))

    @add_log
    def cleanNullOpenInter(self):
        """持仓量为0,用上一个填充"""
        self.paddingWithPrevious("openInterest")

    @add_log
    def cleanNullPriceIndicator(self):
        lastP = self.df["lastPrice"] == 0.0
        high = self.df["highPrice"] == 0.0
        low = self.df["lowPrice"] == 0.0
        bidP = self.df["bidPrice1"] == 0.0
        askP = self.df["askPrice1"] == 0.0
        #如果均为0，删除
        if self.df.loc[lastP & high & low & bidP & askP]._values.any():
            # _values返回array，.any()方法返回True或者False
            for i in self.df.loc[lastP & high & low & bidP & askP].index.values:
                if i not in self.removeList:
                    self.removeList.append(i)
                    logger.info('All Price is Null, remove index = %d' %i)

        # 某些为0，填充
        self.paddingWithPrevious("lastPrice")
        self.paddingWithPrevious("highPrice")
        self.paddingWithPrevious("lowPrice")
        self.paddingWithPrevious("bidPrice1")
        self.paddingWithPrevious("askPrice1")

    @add_log
    def recordExceptionalPrice(self):
        self.estimateExceptional("lastPrice")
        self.estimateExceptional("highPrice")
        self.estimateExceptional("lowPrice")
        self.estimateExceptional("bidPrice1")
        self.estimateExceptional("askPrice1")

    def delItemsFromRemove(self):
        indexList = list(set(self.removeList))
        self.df = self.df.drop(indexList,axis=0)

    def estimateExceptional(self,field):
        dfTemp = pd.DataFrame(self.df[field])
        dfTemp["_id"] = self.df["_id"]
        dfTemp["shift"] = self.df[field].shift(1) # shift把df整体下移一行
        dfTemp["delta"] = abs(dfTemp[field] - dfTemp["shift"])
        dfTemp = dfTemp.dropna(axis=0, how='any')
        dfTemp["IsExcept"] = dfTemp["delta"] >= dfTemp["shift"] * 0.12 # 价格比前一个价格>=1.12倍认为是异常值
        for i, row in dfTemp.loc[dfTemp["IsExcept"]].iterrows():
            if i not in self.removeList:
                self.logList.append(i)
                logger.info('Field = %s, log index = %d, id = %s' % (field, i, row["_id"]))

    def paddingWithPrevious(self,field):
        for i, row in self.df.loc[self.df[field] == 0.0].iterrows():
            if i not in self.removeList:
                preIndex = i - 1
                while(preIndex in self.removeList or preIndex in self.updateList):
                    preIndex = preIndex - 1
                if preIndex >= 0 and i not in self.removeList:
                    row[field] = self.df.loc[preIndex,field]
                    self.df.loc[i,field] = row[field]
                    self.updateList.append(i)
                    logger.info('Field = %s, update index = %d, id = %s' % (field, i, row["_id"]))

    def StandardizeTimePeriod(self,target):
        tar = str(target)
        # target参数应为原始数据中的time值
        ms = 0
        try:
            tp = self.dfInfo.loc[self.Symbol]["CurrPeriod"]
            time1 = [t for i in tp.split(',') for t in i.split('-')]
            # time1是一个形如 ['8:59 ', ' 10:15', ' 10:30 ', ' 11:30', ' 13:30 ', ' 15:00']的列表
            if '.' in tar:
                ms = tar.split('.')[1]
                # 形如vnpy的tick数据中time格式"09:10:16.5",ms为毫秒数
                tar = tar.split('.')[0]

            tar = time.strptime(tar, '%H:%M:%S')
            for i in zip(*([iter(time1)] * 2)):
                # zip形如[('8:59 ', ' 10:15'), (' 10:30 ', ' 11:30'), (' 13:30 ', ' 15:00')]
                start = time.strptime(str(i[0]).strip(), '%H:%M')
                end = time.strptime(str(i[1]).strip(), '%H:%M')
                if self.compare_time(start,end,tar,ms):
                    return True

        except Exception, e:
            print e

    def compare_time(self,s1,s2,st,ms):
        """由于time类型没有millisecond，故单取ms进行逻辑判断"""
        if s2 == time.strptime('00:00', '%H:%M'):
            s2 = time.strptime('23:59:61', '%H:%M:%S')
            # 为了在同一日中，0点时间最大，便于比较
            # 为了比较时间正常，夜盘跨过0点的要分两段写，如：20:59 - 00:00, 00:00 - 2:30
        if st > s1 and st < s2:
            return True
        elif (st == s1 and int(ms) >= 0) or (st == s2 and int(ms) == 0):
            return True
        else:
            return False

# if __name__ == "__main__":
ee = CleanData()
ee.initCleanRegulation()
    # ee.cleanIllegalTradingTime()
ee.reserveLastTickInAuc()
ee.insert2db()
    # ee.cleanSameTimestamp()
    # ee.cleanExceptionalPrice()
    # ee.cleanNullOpenInter()
    # ee.cleanNullPriceIndicator()
    # ee.recordExceptionalPrice()
    # print "Data Clean is completed........."
logger.info("Data Clean is completed.........")

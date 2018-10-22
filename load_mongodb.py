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
import os


def InsertData():
    # mongodb创建用户
    client = pymongo.MongoClient('localhost', 27017)
    # 创建数据库
    IJCAI = client['IJCAI']
    # 创建表
    shop_info_table = IJCAI['shop_info_table']
    shop_info_path = "../dataset/IJCAI-17/dataset/shop_info.txt"
    columnsName = ['shop_id', 'city_name', 'location_id', 'per_pay', 'score', 'comment_cnt', 'shop_level',
                   'cate_1_name', 'cate_2_name', 'cate_3_name']
    # 读写大文件常用方式，当然此文件并不大
    with open(shop_info_path, 'r', encoding='utf-8') as f:
        shop_info_data = []
        for line in f:
            line = line.strip().split(',')
            # 将字段与value组合，然后转换成字典
            shop_info_data.append(dict(zip(columnsName, line)))
    # 每次清除表
    shop_info_table.remove()
    # 插入数据
    shop_info_table.insert(shop_info_data)

    user_pay_table = IJCAI['user_pay_table']
    user_pay_path = "../dataset/IJCAI-17/dataset/user_pay.txt"
    columnsName = ['user_id', 'shop_id', 'time_stamp']
    with open(user_pay_path) as f:
        index = 1
        user_pay_data = []
        # user_pay_table.remove()
        for line in f:
            line = line.strip('\n').split(',')
            user_pay_data.append(dict(zip(columnsName, line)))
            # 文件较大，选择批量插入
            if index == 10000:
                user_pay_table.insert(user_pay_data)
                user_pay_data.clear()
                index = 0
            index += 1

    user_view_table = IJCAI['user_view_table']
    user_view_path = "../dataset/IJCAI-17/dataset/user_view.txt"
    columnsName = ['user_id', 'shop_id', 'time_stamp']
    with open(user_view_path) as f:
        index = 1
        user_view_data = []
        user_view_table.remove()
        for line in f:
            line = line.strip('\n').split(',')
            user_view_data.append(dict(zip(columnsName, line)))
            if index == 10000:
                user_view_table.insert(user_view_data)
                user_view_data.clear()
                index = 0
            index += 1
    # 创建索引
    user_pay_table.create_index('time_stamp')
    user_view_table.create_index('time_stamp')
    print(user_pay_table.index_information())
    print(user_view_table.index_information())


shop_info = {}
user_pay = {}
user_view = {}


# 从数据库加载数据
def loadData():
    client = pymongo.MongoClient('localhost', 27017)
    IJCAI = client['IJCAI']
    shop_info_table = IJCAI['shop_info_table']
    user_pay_table = IJCAI['user_pay_table']
    user_view_table = IJCAI['user_view_table']
    print(shop_info_table.count())
    print(user_pay_table.count())
    print(user_view_table.count())
    global shop_info
    shop_info = shop_info_table.find()
    global user_pay
    startTime = '2015-07-01 00:00:00'
    endTime = '2016-11-01 00:00:00'
    dateRange = pd.date_range(startTime, endTime, freq='MS')
    strSDate = str(dateRange[0])
    strEDate = str(dateRange[1])
    strWhere = "this.time_stamp >=" + '\'' + strSDate + '\'' + "&& this.time_stamp <" + '\'' + strEDate + '\''
    # 可以用类似SQL的形式来select数据


    user_pay = user_pay_table.find({"$where": strWhere}, {"_id": False})
    global user_view
    user_view = user_view_table.find({"$where": strWhere})[:1]
    # 以时间周期来读取数据
    for index, date in enumerate(dateRange):
        strSDate = str(dateRange[index])
        strEDate = str(dateRange[index + 1])
        strWhere = "this.time_stamp >=" + '\'' + strSDate + '\'' + "&& this.time_stamp <" + '\'' + strEDate + '\''
    user_pay = user_pay_table.find({"$where": strWhere})
    path = strSDate[:10] + '.txt'
    fp = open(path, 'w+')
    for x in user_pay:
        x = [x for x in x.values() if type(x) == type('abc')]
        x = ','.join(x)
        fp.write(x)
        fp.write('\n')
        fp.flush()
    fp.close()

'''
#find的具体用法可以在google一下
for u in user_view_table.find()[0:5]:
    print(u)
print('\n')
'''
if __name__ == "__main__":
    loadData()
    shop_info = [x for x in shop_info]
    print(len(shop_info))
    user_pay = [x for x in user_pay]
    user_pay_db = pd.DataFrame
    #    x = pd.Series(x)
    #    print(type(x))
    #    user_pay_db = pd.DataFrame(x)
    user_view = [x for x in user_view]
    print(user_view)
    print(len(user_view))
    print("OK")
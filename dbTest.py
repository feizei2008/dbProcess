#-*- coding: utf-8 -*-
'''
脚本主要实现以下功能：
1、将A库中距今20天以外的数据抽取出来，备份至B中
2、将A中相应数据删除
'''


from pymongo import MongoClient
import datetime
from dateutil import rrule


def get_db(host,port,dbName):
    #建立连接
    client = MongoClient(host,port)
    db = client[dbName]
    return db

def get_all_colls(db):
    return [i for i in db.collection_names()]

def get_specificItems(db,coll_name,time):
    Items = db[coll_name].find({"datetime":{'$lt':time}})
    return Items

def insert_items(db,coll_name,item):
    for i in item:
        id = i["_id"]
        if db[coll_name].count({"_id":id}):
            continue
        else:
            db[coll_name].insert(i)

def remove_items(db,coll_name,item):
    for i in item:
        id = i["_id"]
        try:
            db[coll_name].remove(id)
        except Exception, e:
            print "Error when delete %s, id = %s" %(coll_name, str(id))



if __name__ == "__main__":

    timePoint = datetime.datetime.today() - datetime.timedelta(days = 13)
    db = get_db("192.168.2.48",27017,'MTS_TICK_DB')
    db_bk = get_db("localhost",27017,'MTS_TICK_DB')
    names = get_all_colls(db)
    for i in names:
        items = list(get_specificItems(db,i,timePoint))
        if items != []:
            insert_items(db_bk,i,items)
            # 删除原db中相应数据
            remove_items(db,i,items)
        print "Back up Collection %s........" %(i)


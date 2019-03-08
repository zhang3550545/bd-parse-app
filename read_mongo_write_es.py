#!/usr/bin/env python3      
# -*- coding: utf-8 -*-
import datetime
import sys

import pymongo

sys.path.append(sys.path[0])

from common import es_helper, config


def get_query(date_str):
    date = datetime.datetime.strptime(date_str, "%Y-%m-%d")
    date_lower = date.strftime("%Y-%m-%d %H:%M:%S")
    date_up = (date + datetime.timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    return {"create_time": {"$gte": date_lower, "$lt": date_up}}


def insert_mary_es(es, db, collection_name, date_str, buffersize=1000):
    lists = []
    collection = db[collection_name]
    query = get_query(date_str)
    index_name = collection_name + date_str
    type_name = collection_name
    for i in collection.find(query):
        if len(lists) > buffersize:
            es.insert_mary(index_name, type_name, lists)
            lists.clear()
        lists.append(es_helper.get_source(index_name, type_name, str(i.pop("_id")), i))
    if len(lists) > 0:
        es.insert_mary(index_name, type_name, lists)
        lists.clear()


if __name__ == '__main__':

    if len(sys.argv) <= 1:
        raise Exception("params error. you need add config file path")

    path = sys.argv[1]
    conf = config.init_config(path)

    date = datetime.datetime.today() + datetime.timedelta(days=-1)
    date_str = datetime.datetime(date.year, date.month, date.day).strftime("%Y-%m-%d")

    if len(sys.argv) >= 3:
        date_str = sys.argv[2]

    host = conf.get("mongodb").get("host")
    port = conf.get("mongodb").get("port")
    user = conf.get("mongodb").get("user")
    password = conf.get("mongodb").get("password")
    db = conf.get("mongodb").get("db")
    collection_names = conf.get("mongodb").get("collection")
    buffer_size = int(conf.get("mongodb").get("buffer_size"))

    collection_lists = []
    if "," in collection_names:
        collection_lists = collection_names.split(",")

    es = es_helper.EsHelper(conf)

    client = pymongo.MongoClient(host=host, port=int(port))
    mydb = client['admin']
    mydb.authenticate(user, password)
    mydb = client[db]

    if len(collection_lists) == 0:
        insert_mary_es(es, mydb, collection_names, date_str, buffer_size)
    else:
        for i in collection_lists:
            insert_mary_es(es, mydb, i, date_str, buffer_size)

    client.close()

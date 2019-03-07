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


if __name__ == '__main__':

    if len(sys.argv) <= 1:
        raise Exception("params error. you need add config file path")

    path = sys.argv[1]
    conf = config.init_config(path)

    date = datetime.datetime.today() + datetime.timedelta(days=-1)
    date_str = datetime.datetime(date.year, date.month, date.day).strftime("%Y-%m-%d")

    if len(sys.argv) >= 3:
        date_str = sys.argv[2]

    hosts = conf.get("es").get("hosts")
    settings = conf.get("es").get("settings")
    mappings = conf.get("es").get("mappings")

    host = conf.get("mongodb").get("host")
    port = conf.get("mongodb").get("port")
    user = conf.get("mongodb").get("user")
    password = conf.get("mongodb").get("password")
    db = conf.get("mongodb").get("db")
    collection_name = conf.get("mongodb").get("collection")

    client = pymongo.MongoClient(host=host, port=int(port))
    mydb = client['admin']
    mydb.authenticate(user, password)
    mydb = client[db]
    collection = mydb[collection_name]

    es = es_helper.EsHelper(hosts, settings, mappings)

    lists = []

    print(get_query(date_str))

    for i in collection.find(get_query(date_str)):

        if len(lists) > 1000:
            es.insert_mary(collection_name, "info", lists)
            lists.clear()

        lists.append(es_helper.get_source(collection_name, "info", str(i.pop("_id")), i))

    if len(lists) > 0:
        es.insert_mary(collection_name, "info", lists)
        lists.clear()

    client.close()

#!/usr/bin/env python3      
# -*- coding: utf-8 -*-
import sys

import pymongo

sys.path.append(sys.path[0])

from common import es_helper, config


def get_query():
    return {}


if __name__ == '__main__':

    if len(sys.argv) <= 1:
        raise Exception("params error. you need add config file path")

    path = sys.argv[1]
    conf = config.init_config(path)

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

    for i in collection.find(get_query()):

        if len(lists) > 1000:
            es.insert_mary(collection_name, "info", lists)
            lists.clear()

        lists.append(es_helper.get_source(collection_name, "info", str(i.pop("_id")), i))

    if len(lists) > 0:
        es.insert_mary(collection_name, "info", lists)
        lists.clear()

    client.close()

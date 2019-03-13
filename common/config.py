#!/usr/bin/env python3      
# -*- coding: utf-8 -*-
import configparser


def init_config(path):
    conf = configparser.ConfigParser()
    conf.read(path)
    # mongodb
    host = conf.get("mongodb", "host")
    port = conf.get("mongodb", "port")
    user = conf.get("mongodb", "user")
    password = conf.get("mongodb", "password")
    db = conf.get("mongodb", "db")
    collection = conf.get("mongodb", "collection")
    buffer_size = conf.get("mongodb", "buffer_size")
    # es
    hosts = conf.get("es", "hosts")
    settings = conf.get("es", "settings")
    mappings = conf.get("es", "mappings")
    max_bulk_size = conf.get("es", "max_bulk_size")
    min_bulk_size = conf.get("es", "min_bulk_size")
    min_doc_type = conf.get("es", "min_doc_type")
    vaild_type = conf.get("es", "vaild_type").lower()
    # file
    dir = conf.get("file", "dir")
    # log
    file = conf.get("log", "file")

    return {
        "mongodb": {"host": host, "port": port, "user": user, "password": password, "db": db, "collection": collection,
                    "buffer_size": buffer_size},
        "es": {"hosts": hosts, "settings": settings, "mappings": mappings, "max_bulk_size": max_bulk_size,
               "min_bulk_size": min_bulk_size, "min_doc_type": min_doc_type, "vaild_type": vaild_type},
        "file": {"dir": dir},
        "log": {"file": file}}

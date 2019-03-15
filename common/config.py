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
    min_doc_type = conf.get("es", "min_doc_type").lower()
    vaild_type = conf.get("es", "vaild_type").lower()
    # infile
    dir = conf.get("infile", "dir")

    # outfile
    path = conf.get("outfile", "path")

    # mysql
    mysql_host = conf.get("mysql", "host")
    mysql_port = int(conf.get("mysql", "port"))
    mysql_user = conf.get("mysql", "user")
    mysql_passwd = conf.get("mysql", "passwd")
    mysql_db = conf.get("mysql", "db")

    # log
    file = conf.get("log", "file")

    return {
        "mongodb": {"host": host, "port": port, "user": user, "password": password, "db": db, "collection": collection,
                    "buffer_size": buffer_size},
        "es": {"hosts": hosts, "settings": settings, "mappings": mappings, "max_bulk_size": max_bulk_size,
               "min_bulk_size": min_bulk_size, "min_doc_type": min_doc_type, "vaild_type": vaild_type},
        "infile": {"dir": dir},
        "outfile": {"path": path},
        "mysql": {"host": mysql_host, "port": mysql_port, "user": mysql_user, "passwd": mysql_passwd, "db": mysql_db},
        "log": {"file": file}}

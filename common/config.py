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
    # es
    hosts = conf.get("es", "hosts")
    settings = conf.get("es", "settings")
    mappings = conf.get("es", "mappings")
    # file
    dir = conf.get("file", "dir")

    return {
        "mongodb": {"host": host, "port": port, "user": user, "password": password, "db": db, "collection": collection},
        "es": {"hosts": hosts, "settings": settings, "mappings": mappings},
        "file": {"dir": dir}}

#!/usr/bin/env python3      
# -*- coding: utf-8 -*-
import datetime
import hashlib
import json
import os
import sys

sys.path.append(sys.path[0])

from common import utils, es_helper as eh, config


def buffer_data(data, doc_type, _id, date):
    """
    将数据存储到list
    :param data: 数据
    :param doc_type: doc_type
    :param _id: es中的_id字段
    :param date: 日期
    :return:
    """
    if data.get("message") is None:
        index = doc_type + "request" + date
    else:
        index = doc_type + "response" + date

    lists = res.get(index)
    if lists is None:
        lists = [eh.get_source(index, doc_type, _id, data)]
    else:
        lists.append(eh.get_source(index, doc_type, _id, data))
    if len(lists) > 1000:
        es.insert_mary(index, doc_type, lists)
        lists.clear()
    res[index] = lists


def write_es(file_name, date):
    """
    将数据写入es
    :param file_name: 读取文件的名称
    :param date: 日期
    :return:
    """
    md = hashlib.md5()
    with open(file_name, mode="r") as f:
        for line in f.readlines():
            md.update(line[24:].encode())
            _id = md.hexdigest()
            dicts = json.loads(line[24:])
            dicts["dzjk_create_time"] = line[:23]
            dicts["dzjk_update_time"] = line[:23]
            service_code = dicts["header"]["serviceCode"].lower()
            buffer_data(utils.clear_json(data=dicts), service_code, _id, date)

        for line in res:
            if "request" in line:
                print(line[:-15])
                es.insert_mary(line, line[:-15], res[line])
            elif "response" in line:
                es.insert_mary(line, line[:-16], res[line])


if __name__ == '__main__':

    if len(sys.argv) <= 1:
        raise Exception("params error. you need add config file path")

    config_path = sys.argv[1]
    conf = config.init_config(config_path)

    path = conf.get("file").get("dir")

    yesterday = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime("%Y%m%d")
    es = eh.EsHelper(conf)
    res = {}

    dirs = os.listdir(path)
    for i in dirs:
        write_es(path + "/" + i, yesterday)

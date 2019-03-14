#!/usr/bin/env python3      
# -*- coding: utf-8 -*-
import datetime
import hashlib
import json
import os
import sys
import time

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
    bulk_size = int(conf.get("es").get("max_bulk_size"))
    min_bulk_size = int(conf.get("es").get("min_bulk_size"))
    min_doc_type = conf.get("es").get("min_doc_type")

    if data.get("message") is None:
        index = doc_type + "request" + date
    else:
        index = doc_type + "response" + date
        if min_doc_type == doc_type or doc_type in min_doc_type:
            bulk_size = min_bulk_size

    lists = res.get(index)
    if lists is None:
        lists = [eh.get_source(index, doc_type, _id, data)]
    else:
        lists.append(eh.get_source(index, doc_type, _id, data))

    # 记录index name的count数，用于校验数据的一致性
    dict_count[index] = dict_count.get(index, 0) + 1

    if len(lists) > bulk_size:
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
            if service_code in vaild_type or vaild_type == "all":  # 使用vaild_type，指定需要搜集哪种渠道的数据，all表示所有
                buffer_data(utils.clear_json(data=dicts), service_code, _id, date)

        for line in res:
            doc_type = utils.get_doc_type(index_name=line)
            es.insert_mary(line, doc_type, res[line])


if __name__ == '__main__':

    if len(sys.argv) <= 1:
        raise Exception("params error. you need add config file path")

    config_path = sys.argv[1]
    conf = config.init_config(config_path)
    path = conf.get("infile").get("dir")
    vaild_type = conf.get("es").get("vaild_type")

    date_str = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

    if len(sys.argv) >= 3:
        date_str = sys.argv[2]

    path = path + "/" + date_str

    es = eh.EsHelper(conf)
    res = {}

    dict_count = {}

    dirs = os.listdir(path)
    for i in dirs:
        write_es(path + "/" + i, date_str.replace("-", ""))

    time.sleep(60)

    for i in dict_count:
        es.check(i, dict_count[i])

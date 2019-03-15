#!/usr/bin/env python3      
# -*- coding: utf-8 -*-
import sys
import time

from common import config, utils
from common.es_helper import EsHelper
import common.es_helper as eh

sys.path.append(sys.path[0])

if __name__ == '__main__':

    """
    按月归档的脚本
    """

    if len(sys.argv) <= 2:
        raise Exception("参数错误.需要传入配置文件路径和年月. 例如：./common/dev_config.ini 201901")

    config_path = sys.argv[1]
    month = sys.argv[2]

    conf = config.init_config(config_path)
    size = conf.get("es").get("max_bulk_size")
    min_bulk_size = conf.get("es").get("min_bulk_size")
    min_doc_type = conf.get("es").get("min_doc_type")

    es = EsHelper(conf)
    # 获取指定的month的index
    result = es.cat_indices_list(month=month)

    res = {}
    month_indexs = set()  # 记录month_index的索引，用来计算插入后的count数
    for i in result:
        month_index = i[:-2]
        month_indexs.add(month_index)
        # 指定的doc_type，设置最小的buffer size
        if utils.get_doc_type(i) in min_doc_type:
            size = min_bulk_size

        # 查询所有的index的数据
        for r in es.search(index_name=i):
            body = r["_source"]
            _id = r["_id"]
            # 插入数据
            month_index_list = res.get(month_index, [])
            doc_type = utils.get_doc_type(index_name=month_index, month=True)
            month_index_list.append(eh.get_source(month_index, doc_type, _id, body))
            res[month_index] = month_index_list

            # 批量插入
            if len(month_index_list) > size:
                es.insert_mary(index_name=month_index, doc_type=doc_type, actions=month_index_list)
                month_index_list.clear()
                res[month_index] = month_index_list

    for i in res:
        actions = res[i]
        if len(actions) > 0:
            es.insert_mary(index_name=i, doc_type=utils.get_doc_type(index_name=i, month=True), actions=actions)
            actions.clear()

    # 休息60s，等待数据插入
    time.sleep(60)

    # check 原始的index数量和插入后month_index的数量
    original_count = es.count_list(result)
    end_count = es.count_list(month_indexs)

    if original_count == end_count:
        # 如果数量相等，证明数据已经归档好了，那么就删除原始数据
        for i in result:
            es.delete_all(index_name=i, doc_type=utils.get_doc_type(index_name=i))

#!/usr/bin/env python3      
# -*- coding: utf-8 -*-
import sys

from common import config, utils
from common.es_helper import EsHelper
import common.es_helper as eh

sys.path.append(sys.path[0])
if __name__ == '__main__':

    if len(sys.argv) <= 1:
        raise Exception("params error. you need add config file path")

    config_path = sys.argv[1]
    month = sys.argv[2]

    conf = config.init_config(config_path)

    es = EsHelper(conf)

    result = es.cat_indices_list(month=month)

    res = {}
    count = 0
    s = set()
    for i in result:
        month_index = i[:-2]
        s.add(month_index)
        for r in es.search(index_name=i):
            count = count + 1
            body = r["_source"]
            _id = r["_id"]
            # 插入数据
            month_index_list = res.get(month_index, [])
            doc_type = utils.get_doc_type(index_name=month_index, month=True)
            month_index_list.append(eh.get_source(month_index, doc_type, _id, body))
            res[month_index] = month_index_list

            if len(month_index_list) > 100:
                es.insert_mary(index_name=month_index, doc_type=doc_type, actions=month_index_list)
                month_index_list.clear()
                res[month_index] = month_index_list

    for i in res:
        actions = res[i]
        if len(actions) > 0:
            es.insert_mary(index_name=i, doc_type=utils.get_doc_type(index_name=i, month=True), actions=actions)
            actions.clear()

    original_count = 0
    for i in result:
        original_count = original_count + es.count(i)

    _count = 0

    for i in s:
        _count = _count + es.count(i)

    print("count: " + str(count) + "    original_count: " + str(original_count) + "     _count: " + str(_count))

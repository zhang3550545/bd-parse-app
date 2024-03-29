#!/usr/bin/env python3      
# -*- coding: utf-8 -*-
import elasticsearch
from elasticsearch.helpers import bulk
from common.logger import Log


def get_source(index_name, doc_type, _id, data):
    """
    获取一条数据的完整dict
    :param index_name: index名称
    :param doc_type: type名称
    :param _id: json数据的md5值，保证同一条数据的唯一
    :param data: 需要插入的数据
    :return:
    """
    return {"_op_type": "index", "_index": index_name, '_type': doc_type, "_id": _id, "_source": data}


class EsHelper:
    def __init__(self, config):
        self.es = elasticsearch.Elasticsearch(hosts=config.get("es").get("hosts"))
        self.settings = config.get("es").get("settings")
        self.mappings = config.get("es").get("mappings")
        log_file = config.get("log").get("file")
        self.max_bulk_size = config.get("es").get("max_bulk_size")
        self.min_bulk_size = config.get("es").get("min_bulk_size")
        self.min_doc_type = config.get("es").get("min_doc_type")
        self.log = Log(log_file)

    def insert_mary(self, index_name, doc_type, actions):
        """
        bulk 批量插入数据到es
        :param index_name:
        :param doc_type:
        :param actions: 数据，list[dict]数据
        :return:
        """
        if self.es.indices.exists(index=index_name) is not True:
            self.es.indices.create(index=index_name, body=self.settings)
            self.es.indices.put_mapping(index=index_name, doc_type=doc_type, body=self.mappings)
        try:
            bulk(self.es, actions=actions, index=index_name)
        except:
            self.log.info("index_name: " + index_name)

    def search(self, index_name):
        size = self.max_bulk_size
        if index_name in self.min_doc_type:
            size = self.min_bulk_size
        size = int(size)
        query_data = self.es.search(index=index_name, scroll='10m', timeout='10s', size=size)
        data = query_data.get("hits").get("hits")
        if not data:
            self.log.info("index_name: %s search not data" % index_name)
        scroll_id = query_data["_scroll_id"]
        total = query_data["hits"]["total"]
        for i in range(int(total / size)):
            res = self.es.scroll(scroll_id=scroll_id, scroll='10m')
            data += res["hits"]["hits"]

        return data

    def count(self, index_name):
        """
        获取index name的count数量
        :param index_name:
        :return: count
        """
        count = self.es.count(index=index_name)
        return count.get("count")

    def count_list(self, index_name_list):
        """
        计算多个index的count数量
        :param index_name_list:
        :return: count
        """
        count = 0
        for i in index_name_list:
            count = count + self.count(i)
        return count

    def check(self, index_name, original_count):
        """
        检查数据插入的是否一致
        :param index_name:
        :param orginal_count:
        :return:
        """
        es_count = self.count(index_name)
        if es_count != original_count:
            self.log.info(
                "index_name：%s 数据不一致，original count：%s，es count：%s" % (index_name, str(original_count), str(es_count)))
        else:
            self.log.info("index_name：%s 数据一致,写入es成功" % index_name)

    def cat_indices_list(self, month=""):
        """
        获取所有index的列表
        :return: list
        """
        result = []
        res = self.es.cat.indices()
        lines = res.split("\n")[:-1]
        for i in lines:
            index_name = i.split(" ")[2]
            if "." not in index_name and month in index_name:
                result.append(index_name)

        return result

    def delete_all(self, index_name, doc_type):
        """
        删除所有的doc数据
        :param index_name:
        :param doc_type:
        :return:
        """
        query_all = {"query": {"match_all": {}}}
        self.es.delete_by_query(index=index_name, body=query_all, doc_type=doc_type)

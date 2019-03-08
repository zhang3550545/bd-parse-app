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
        except Exception as e:
            self.log.info(index_name)
            self.log.info(e)

#!/usr/bin/env python3      
# -*- coding: utf-8 -*-
import json
import os


def clear_list_element(data):
    """
    清除datas中的list空集合，字典数据
    :param data: 数据源
    :return: 返回的数据
    """
    if isinstance(data, list):
        for i in data:
            if i is None or i == {} or i == []:
                data.remove(i)
            elif isinstance(i, dict):
                values_copy = clear_dict_element(i)
                index = data.index(i)
                data.remove(i)
                data.insert(index, values_copy)
            elif isinstance(i, list):
                clear_list_element(i)
    return data


def clear_dict_element(data):
    """
    清除datas中的dict空集合，空字典数据
    :param data: 数据源
    :return: 数据
    """
    datas_copy = data.copy()
    if isinstance(data, dict):
        for i in data:
            value = data.get(i)
            if value is None or value == {} or value == []:
                datas_copy.pop(i)
            elif isinstance(value, dict):
                values_copy = clear_dict_element(value)
                datas_copy[i] = values_copy
            elif isinstance(value, list):
                clear_list_element(value)

    return datas_copy


def clear_json(data):
    """
    清理数据源中的空list和空dict
    :param data: 数据源
    :return: 数据
    """
    if isinstance(data, list):
        data = clear_list_element(data)
    elif isinstance(data, dict):
        data = clear_dict_element(data)

    return data


def get_doc_type(index_name, month=False):
    doc_type = ""
    if "request" in index_name:
        if month:
            doc_type = index_name[:-13]
        else:
            doc_type = index_name[:-15]
    elif "response" in index_name:
        if month:
            doc_type = index_name[:-14]
        else:
            doc_type = index_name[:-16]

    return doc_type


def get_file_path(path):
    """
    获取所有文件路径
    :param path: 路径
    :return: 路径列表
    """
    file_paths = []
    if os.path.isfile(path):
        file_paths.append(path)
    elif os.path.isdir(path):
        for i in os.listdir(path):
            paths = get_file_path(path + "/" + i)
            file_paths = file_paths + paths

    return file_paths


def create_table_sql(table_name, line):
    """
    拼接创建sql的语句
    :param table_name: 表名
    :param line: json数据
    :return: create sql语句
    """
    sql = "create table if not exists %s ( id bigint(32) not null auto_increment," % table_name
    dicts = json.loads(line)
    for i in dicts:
        if isinstance(dicts[i], int):
            sql = sql + i + " bigint(32),"
        elif isinstance(dicts[i], str):
            sql = sql + i + " varchar(255),"
        else:
            sql = sql + i + " varchar(255),"
    sql = sql + "create_time datetime not null default CURRENT_TIMESTAMP,update_time datetime not null default CURRENT_TIMESTAMP,"
    sql = sql + "PRIMARY KEY (id))ENGINE=InnoDB DEFAULT CHARSET=utf8;"
    return sql


def insert_into_sql(table_name, line):
    keys = ""
    values = ""
    for i in json.loads(line):
        keys = keys + i + ","
        values = values + "%s,"

    return "insert into %s(%s) values(%s);" % (table_name, keys[:-1], values[:-1])

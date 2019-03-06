#!/usr/bin/env python3      
# -*- coding: utf-8 -*-

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

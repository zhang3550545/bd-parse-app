#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import datetime
import json
import sys

from common import utils, config
from common.mysql_helper import MySQLHelper

sys.path.append(sys.path[0])

if __name__ == '__main__':

    """
    调用方式：python read_json_to_mysql.py config_path task_name date[可选]
    参数：  
        config_path: 配置文件路径
        task_name: 任务名称
        date：格式如：20190101，默认昨天的日期
    """

    if len(sys.argv) <= 3:
        raise Exception("参数不对")

    config_path = sys.argv[1]
    conf = config.init_config(config_path)
    m = MySQLHelper(conf)

    # 从配置文件中获取对应的路径
    path = conf.get("outfile").get("path")

    task_name = sys.argv[2]

    # 默认昨天的日期
    date = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime("%Y%m%d")
    if len(sys.argv) >= 4:
        date = sys.argv[3]

    # 拼接spark sql任务落地的文件路径：任务名+日期+表名（不包含.json这样的后缀）
    path = path + "/" + task_name + "/" + date
    paths = utils.get_file_path(path)

    insert_sqls = {}

    for i in paths:
        table_name = i.split("/")[-1]
        with open(i, mode="r", encoding="utf-8") as f:
            values = []
            for i in f:
                insert_sql = insert_sqls.get(table_name)
                if insert_sql == "" or insert_sql is None:  # 创建插入语句
                    insert_sql = m.get_insert_table_sql(table_name)
                    insert_sqls[table_name] = insert_sql

                dicts = json.loads(i)
                value = m.get_row(table_name, date, dicts)
                values.append(value)

                if len(values) > 1000:
                    m.executemany(insert_sqls.get(table_name), values)
                    values.clear()

            if len(values) > 0:
                m.executemany(insert_sqls.get(table_name), values)
                values.clear()

    m.close()

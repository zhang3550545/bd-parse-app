#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import sys

from common import utils, config
from common.mysql_helper import MySQLHelper

sys.path.append(sys.path[0])

if __name__ == '__main__':

    if len(sys.argv) <= 2:
        raise Exception("参数不对")

    config_path = sys.argv[1]
    conf = config.init_config(config_path)
    m = MySQLHelper(conf)

    path = sys.argv[2]
    table_name = path.split("/")[-1]

    paths = utils.get_file_path(path)

    with open(paths[0], mode="r") as f:
        line = f.readline()
        create_sql = utils.create_table_sql(table_name, line)
        m.create_table(create_sql)
        insert_sql = utils.insert_into_sql(table_name, line)

    values = []
    for i in paths:
        with open(i, mode="r") as f:
            for i in f.readlines():
                value = []
                dicts = json.loads(i)
                for j in dicts:
                    value.append(dicts[j])

                values.append(value)

                if len(values) > 1000:
                    m.executemany(insert_sql, values)
                    values.clear()

    if len(values) > 0:
        m.executemany(insert_sql, values)
        values.clear()

    m.close()

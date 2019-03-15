#!/usr/bin/env python3      
# -*- coding: utf-8 -*-
import pymysql


class MySQLHelper:

    def __init__(self, conf):
        host = conf.get("mysql").get("host")
        port = conf.get("mysql").get("port")
        user = conf.get("mysql").get("user")
        passwd = conf.get("mysql").get("passwd")
        db = conf.get("mysql").get("db")
        self.conn = pymysql.Connect(host=host, port=port, user=user, passwd=passwd, db=db)
        self.conn.autocommit(False)
        self.cur = self.conn.cursor()

    def create_table(self, sql):
        return self.cur.execute(sql)

    def execute(self, sql):
        self.cur.execute(sql)
        self.conn.commit()

    def executemany(self, sql, values):
        self.cur.executemany(sql, values)
        self.conn.commit()

    def close(self):
        self.cur.close()
        self.conn.close()

    def get_table_coulmns_sql(self, table):
        """
        获取表的列名
        :param table:
        :return:
        """
        self.cur.execute("select COLUMN_NAME from information_schema.COLUMNS where table_name = '%s';" % table)
        columns = []
        for i in self.cur:
            if i[0] == "id" or i[0] == "create_time" or i[0] == "update_time":
                continue
            columns.append(i[0])
        return columns

    def get_insert_table_sql(self, table):
        """
        获取插入语句
        :param table:
        :return:
        """
        columns = self.get_table_coulmns_sql(table)
        print(columns)
        keys = ""
        values = ""
        for i in columns:
            keys = keys + i + ","
            values = values + "%s,"
        return "insert into %s(%s) values(%s);" % (table, keys[:-1], values[:-1])

    def get_row(self, table, date, res):
        """
        通过表的columns，来指定values
        :param table: 表名
        :param date: partition_key分区的key
        :param res: 一行数据
        :return:
        """
        columns = self.get_table_coulmns_sql(table)
        row = []
        for i in columns:
            if i == "partition_key":
                row.append(date)
            else:
                value = res.get(i, "")
                row.append(value)
        return row

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

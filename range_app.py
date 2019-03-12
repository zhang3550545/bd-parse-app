#!/usr/bin/env python3      
# -*- coding: utf-8 -*-
import datetime
import os
import sys

if __name__ == '__main__':
    """
    批量补数据的逻辑
    脚本执行命令：python  range_app.py 2019-01-01 2019-02-01
    """

    print(sys.argv)

    if len(sys.argv) == 3:
        start_date = datetime.datetime.strptime(sys.argv[1], "%Y-%m-%d")
        end_date = datetime.datetime.strptime(sys.argv[2], "%Y-%m-%d")

        while start_date < end_date:
            shell = "sh app.sh %s" % start_date.strftime("%Y-%m-%d")
            print(shell)
            os.system(shell)
            start_date = start_date + datetime.timedelta(days=1)

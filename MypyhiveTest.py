#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time : 2019/5/22 12:25
@Author : Administrator
@Email : xxxxxxxxx@qq.com
@File : MypyhiveTest.py
@Project : untitled
"""

from HiveConn.MyPyhiveConn import Hive

hive = Hive()
sqlAll = "select userid,count(*) " \
         "from sogoinfo " \
         "group by userid"

result = hive.getAll(sqlAll)
if result:
    print("get all")
    for row in result:
        # print("%s\t%s" % (row[1], row[2]))
        print(row)

hive.dispose()
#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time : 2019/5/22 11:26
@Author : Administrator
@Email : xxxxxxxxx@qq.com
@File : MyPyhiveConn.py
@Project : untitled
"""

from pyhive import hive
from TCLIService.ttypes import TOperationState
from DBUtils.PooledDB import PooledDB

from HiveConn import Config


class Hive(object):
    """
        hive数据仓库对象，负责产生数据库连接 , 此类中的连接采用连接池实现获取连接对象：conn = Hive.getConn()
            释放连接对象;conn.close()或del conn
    """
    # 连接池对象
    __pool = None

    def __init__(self):
        # 数据库构造函数，从连接池中取出连接，并生成操作游标
        self._conn = Hive.__getConn()
        self._cursor = self._conn.cursor()

    @staticmethod
    def __getConn():
        """

        :return: pyhvie.connection
        """
        if Hive.__pool is None:
            __pool = PooledDB(creator=hive, mincached=1, maxcached=5,
                              host=Config.HOST,
                              port=Config.PORT,
                              username=Config.USER,
                              # password=Config.PASSWORD,
                              database=Config.DATABASE,
                              auth=Config.AUTH)
        return __pool.connection()

    def getAll(self, sql, param=None):
        """
        @summary: 执行查询，并取出所有结果集
        @param sql:查询ＳＱＬ，如果有查询条件，请只指定条件列表，并将条件值使用参数[param]传递进来
        @param param: 可选参数，条件列表值（元组/列表）
        @return: result list(字典对象)/boolean 查询到的结果集
        """
        # if param is None:
        #     count = self._cursor.execute(sql)
        #     print(count)
        # else:
        #     count = self._cursor.execute(sql, param)
        # if count > 0:
        #     result = self._cursor.fetchall()
        # else:
        #     result = False
        try:
            self._cursor.execute(sql, param, async=True)
            status =self._cursor.poll().operationState
            print("status:", status)

            # 如果执行出错，循环执行，直到执行正确，可不要
            while status in (TOperationState.INITIALIZED_STATE, TOperationState.RUNNING_STATE):
                logs = self._cursor.fetch_logs()
                for message in logs:
                    print(message)
                # If needed, an asynchronous query can be cancelled at any time with:
                self._cursor.cancel()
                # status = cursor.poll().operationState

            result = self._cursor.fetchall()
        except Exception as e:
            raise e
        return result

    def begin(self):
        """
        @summary: 开启事务
        """
        self._conn.autocommit(0)

    def end(self,option='commit'):
        """
        @summary: 结束事务
        """
        if option=='commit':
            self._conn.commit()
        else:
            self._conn.rollback()

    def dispose(self,isEnd=1):
        """
        @summary: 释放连接池资源
        """
        if isEnd==1:
            self.end('commit')
        else:
            self.end('rollback')
        self._cursor.close()
        self._conn.close()

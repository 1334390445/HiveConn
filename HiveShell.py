#! /usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
import os,sys
import time


clusterB = u"""beeline -u "jdbc:hive2://bigdata-cdh01:10000/default;principal=yyy?tez.queue.name=xxx" --showHeader={} --outputformat=csv2 -e """
clusterE = u"""beeline -u "jdbc:hive2://bigdata-cdh02:10000/default;principal=yyy?tez.queue.name=xxx" --showHeader={} --outputformat=csv2 -e """



def execute(sql,cluster='b',showHeader=True,path=None,is_break=True):
    """
    
    :param sql: 
    :param cluster: 指定集群b或e。
    :param showHeader: 是否显示表头字段。
    :param path: 保存路径，默认None，不保存查询结果；需要保存到指定文件时，设置路径，例如path = ">zhl/test.txt"，注意>是linux的覆写的意思，也可用>>,追加写入。
    :param is_break: 当当前sql任务执行失败时，若is_break=True，则退出程序，不再执行下一个sql；否则，即使当前sql执行失败，也继续执行下一个sql任务。
    :return: 
    """
    hql = sql.strip()
    hql_cp = ''

    # 去除sql中每行前后的的tab键
    for line in hql.split('\n'):
        hql_cp = hql_cp + '\n' + line.strip()

    # 判断showHeader是否为bool值，否则抛出错误
    if type(showHeader) is bool:
        if showHeader is True:
            _showHeader = 'true'
        else:
            _showHeader = 'false'
    else:
        raise ValueError('showHeader只能为True或者False')

    # 判断cluster是否为b或e，否则抛出错误
    if cluster != 'b' and cluster != 'e':
        raise ValueError("cluster 只能为'b'或'e' !")


    if hql_cp:

        if cluster == 'b':
            cmd = clusterB.format(_showHeader)
        elif cluster == 'e':
            cmd = clusterE.format(_showHeader)

        cmd = cmd + '"{}"'.format(hql_cp)

        # 拼接保存数据的文件路径命令
        if path is not None:
            cmd = cmd + path.strip()

        # print(cmd)
        try:
            # 任务执行开始时间
            start_time = time.time()
            # 格式化成2016-03-20 11:45:39形式
            print('starttime：',time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start_time)))

            # 执行任务
            is_success = os.system(cmd.strip())

            # 任务执行开始时间
            end_time = time.time()
            # 格式化成2016-03-20 11:45:39形式
            print('endtime：',time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end_time)))
            if is_success == 0:
                print("任务执行成功！")
            else:
                print("任务执行失败！")

                # 当is_break=True，退出程序
                if is_break is True:
                    sys.exit()
        except Exception as e:
            raise e
    else:
        raise ValueError(u'请传入正确的sql！')

if __name__ == '__main__':


    sql = """
    select words,count(1) word_cnt
    from(
        select explode(split(keyword,'')) words
        from mlte_s1u_http
        where slicetime between '2019020500' and '2019020523'
        and keyword is not null
        and keyword<>'NULL'
        and keyword<>'null'
        and keyword<>''
    )a1
    group by words
    order by word_cnt desc
    limit 20;
    """

    path = ">zhl/test.txt"
    execute(sql,cluster='b',showHeader=True,path=path)




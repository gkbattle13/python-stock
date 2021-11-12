#!/usr/bin/env python
# -*- coding: utf-8 -*-
# import pandas as pd
import inspect
import os
import sys
import time

from tushare_data import configuration
import pandas as pd

from tushare_data.data.box2 import market_data, hKong

current_path = inspect.getfile(inspect.currentframe())
# 获取当前文件所在目录，相当于当前文件的父目录
dir_name = os.path.dirname(current_path)
# 转换为绝对路径
file_abs_path = os.path.abspath(dir_name)
# 划分目录，比如a/b/c划分后变为a/b和c
list_path = os.path.split(file_abs_path)
print("添加包路径为：" + list_path[0])
sys.path.append(list_path[0])


def quotes_all(engine, pro, logger):
    sql = 'select cal_date from basic_trade_cal where is_open = 1 and cal_date >= 20170101 and cal_date <= 20180517'
    data_1 = pd.read_sql(sql, engine)

    market_data_e = market_data.market_data(engine, pro, logger)

    for index, row in data_1.iterrows():
        date = row["cal_date"]

        # 个股资金走向   以下获取日期为 20170101 到 20191231
        # 港股通每日成交统计  被限制为一分钟两次
        market_data_e.ggt_daily(trade_date=date)
        time.sleep(30)

        # 备用行情 每分钟10次，每天500次  20180101  到 20200923
        market_data_e.bak_daily(trade_date=date)
        time.sleep(6)


def quotes_hk(engine, pro, logger):
    hKong_e = hKong.hKong(engine, pro, logger)
    sql = 'select cal_date from hk_tradecal where is_open = 1 and cal_date >= 20190301 and cal_date <= 20210310'
    data_1 = pd.read_sql(sql, engine)

    for index, row in data_1.iterrows():
        date = row["cal_date"]

        # 个股资金走向   以下获取日期为 20170101 到 20191231
        # 港股通每日成交统计  被限制为一分钟两次
        hKong_e.hk_daily(trade_date=date)
        time.sleep(3)



def run():
    engine, pro, logger = configuration.sql_tuShare_log('config-local.ini')
    # hKong_e = hKong.hKong(engine, pro, logger)
    # hKong_e.hk_tradecal( start_date="20190101", end_date="20200711")

    # 备用行情
    # quotes_all(engine, pro, logger)

    # 该接口每天获取上线为500
    # market_data_entry = market_data.market_data(engine, pro, logger)
    # market_data_entry.bak_daily(trade_date="20190708")

    # market_data_entry.ggt_daily(trade_date="20190708")

    quotes_hk(engine, pro, logger)

run()

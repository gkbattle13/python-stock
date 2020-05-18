#!/usr/bin/env python
# -*- coding: utf-8 -*-
# import pandas as pd
import queue
import sys
import threading

import tushare as ts
from sqlalchemy import create_engine

from tushare_data.data.box2 import basic
from tushare_data.data.box2 import market_data
from tushare_data.data.box2.fund import fund
from tushare_data.data.box2.quotes import quotes
from tushare_data.utils import loggerUtils
# 该类获取2003 SARS 期间的数据

ts.set_token('')
pro = ts.pro_api()
logger = loggerUtils.TNLog()
engine = create_engine('mysql+pymysql://root:rootroot@localhost:3306/stock?charset=UTF8MB4', encoding='utf-8',pool_size=30, max_overflow=30)
# python 2.7用法
# engine = create_engine('mysql://root:rootroot@localhost:3306/stock?charset=utf8', pool_size=30, max_overflow=30)

# 获取基础数据
def base():
    basic_entry = basic.basic(engine, pro, logger)

    # 获取股票基础数据  每天刷一次
    # basic_entry.stock_basic(None, None, None)

    # 获取交易日  已经获取到20201231号，一年获取一次
    # basic_entry.trade_Cal()
    # basic_entry.trade_Cal(exchange='SSE')
    # basic_entry.trade_Cal(exchange='SZSE')
    # basic_entry.trade_Cal(exchange='CFFEX')
    # basic_entry.trade_Cal(exchange='SHFE')
    # basic_entry.trade_Cal(exchange='CZCE')
    # basic_entry.trade_Cal(exchange='DCE')
    # basic_entry.trade_Cal(exchange='INE')
    # basic_entry.trade_Cal(exchange='IB')
    # basic_entry.trade_Cal(exchange='XHKG')
    #
    # 获取沪深股通成份股
    # basic_entry.hs_const("SH")
    # basic_entry.hs_const("SZ")
    #
    # basic_entry.stock_company(exchange='SSE');
    # basic_entry.stock_company(exchange='SZSE');
    # 获取上市公司管理层
    basic_entry.stk_managers("600660.SH")
    # 获取管理层薪酬和持股
    basic_entry.stk_rewards(ts_code="600660.SH")


# 获取行情数据
def makret_data():
    # 获取指定时间段之内的所有股票数据
    makret_data1 = market_data.makret_data(engine, pro, logger)
    # makret_data1.daily_cycle(exchange='SSE', start_date="20020601", end_date="20040601")
    # 获取指定时间段之内所有股票的周数据
    # makret_data1.weekly_cycle(exchange='SSE', start_date="20030113", end_date="20040601")
    # makret_data1.monthly_cycle(exchange='SSE', start_date="20030515", end_date="20040601")

    # 获取指定时间段内沪深股通十大成交股  由于积分限制每分钟只能调取200次
    # makret_data1.hsgt_top10_cycle(exchange='SSE', start_date="20190101", end_date="20200228",sleep = 0.3)
    # 获取指定时间段内沪深港通资金流向
    # makret_data1.moneyflow_hsgt_cycle(exchange='SSE', start_date="20190101", end_date="20200228")
    # 获取指定时间段内个股资金流向
    # makret_data1.money_flow_cycle(exchange='SSE', start_date="20190101", end_date="20200228")

    # makret_data1.ggt_daily_cycle(exchange='SSE', start_date="20190101", end_date="20200228", sleep = 30)

base()
# makret_data()

#!/usr/bin/env python
# -*- coding: utf-8 -*-
# import pandas as pd
import queue
import threading

import tushare as ts
from sqlalchemy import create_engine
from sqlalchemy.exc import ResourceClosedError

from tushare_data.data.box2 import basic
from tushare_data.data.box2 import market_data
from tushare_data.data.box2.fund import fund
from tushare_data.data.box2.quotes import quotes
from tushare_data.utils import loggerUtils

# 该类获取2003 SARS 期间的数据

ts.set_token('')
pro = ts.pro_api()
logger = loggerUtils.TNLog()
engine = create_engine('mysql://root:rootroot@localhost:3306/stock?charset=utf8', pool_size=30, max_overflow=30)


# 获取基础数据
def base():
    basic_entry = basic.basic(engine, pro, logger)
    # 获取股票基础数据
    basic_entry.stock_basic(None, None, None)
    # 获取交易日
    basic_entry.trade_Cal(exchange='SSE')
    basic_entry.trade_Cal(exchange='SZSE')
    basic_entry.trade_Cal(exchange='CFFEX')
    basic_entry.trade_Cal(exchange='SHFE')
    basic_entry.trade_Cal(exchange='CZCE')
    basic_entry.trade_Cal(exchange='DCE')
    basic_entry.trade_Cal(exchange='INE')
    basic_entry.trade_Cal(exchange='IB')
    basic_entry.trade_Cal(exchange='XHKG')


# 获取行情数据
def makret_data():
    # 获取指定时间段之内的所有股票数据
    makret_data1 = market_data.makret_data(engine, pro, logger)
    # makret_data1.daily_cycle(exchange='SSE', start_date="20020601", end_date="20040601")
    makret_data1.weekly_cycle(exchange='SSE', start_date="20030113", end_date="20040601")
    # makret_data1.monthly_cycle(exchange='SSE', start_date="20030515", end_date="20040601")

# base()
makret_data()

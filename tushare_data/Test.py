#!/usr/bin/env python
# -*- coding: utf-8 -*-
# import pandas as pd
import inspect
import os
import queue
import sys
import threading

current_path = inspect.getfile(inspect.currentframe())
# 获取当前文件所在目录，相当于当前文件的父目录
dir_name = os.path.dirname(current_path)
# 转换为绝对路径
file_abs_path = os.path.abspath(dir_name)
# 划分目录，比如a/b/c划分后变为a/b和c
list_path = os.path.split(file_abs_path)
print("添加包路径为：" + list_path[0])
sys.path.append(list_path[0])


# 获取当前文件路径
from tushare_data import configuration
import tushare as ts
from sqlalchemy import create_engine

from tushare_data.data.box2 import basic, market_data
from tushare_data.data.box2.fund import fund
from tushare_data.data.box2.quotes import quotes
from tushare_data.utils import loggerUtils

"""
1. 开始时间为19901219 结束时间为 20180907
"""

# # 测试接口是否是好的
# def base():
#     engine, pro, logger = configuration.sql_tuShare_log()
#     # 基础数据测试
#     basic_entry = basic.basic(engine, pro, logger)
#     basic_entry.stock_basic(None, None, None)  # 股票列表
#     basic_entry.hs_const("SH", None)  # 沪深股通成份股
#     basic_entry.hs_const("SZ", None)  # 沪深股通成份股
#     basic_entry.stock_company(None, None)  # 上市公司基本信息
#
#     # 行情数据
#     # quotes_e = quotes(engine, pro, logger)
#     # quotes_e.quotes_daily(trade_date="20181102")
#     # quotes_e.adj_trade(trade_date="20181102")
#     # quotes_e.suspend(ts_code="600848.SH")
#     # quotes_e.get_daily_basic(trade_date="20181102")
#
#
# # 处理基础数据 初始化版本   获取到20181031为止
# queue_history = queue.Queue()
#
# # 获取行情数据
# quotes_e = quotes(engine, pro, logger)
#
#
# def getHistory(max_thread, start_date, end_date):
#     basic_e = basic.basic(engine, pro, logger)
#     # 1 获取当前股票列表
#     basic_e.stock_basic()
#     # 2 交易日历，获取19901219到 未知的数据（end_date tushare会变动，tushare限制为1W条） 数据库存放为到20191231,获取07年开始到现在的
#     basic_e.trade_Cal(start_date=start_date)
#     # 3 获取公司信息
#     basic_e.stock_company()
#     # 4 获取沪深股通成份股
#     basic_e.hs_const(hs_type='SH')
#     basic_e.hs_const(hs_type='SZ')
#     # 5 获取曾用名
#     basic_e.name_change()
#     # 6 获取IPO新股
#     basic_e.new_share(start_date=start_date)
#     quotes_history(max_thread, start_date, end_date)
#
#
# def getTradeCal(startDate, endDate, type):
#     sql2 = "select cal_date from basic_trade_cal where cal_date >='" + startDate + "' and cal_date <= '" + endDate + "'and is_open = '" + type + "'"
#     # data = pd.read_sql(sql2, engine)
#     # train_data = pd.np.array(data)  # np.ndarray()
#     # day_list = train_data.tolist()  # list
#     # day_list = data['cal_date'].values.tolist()
#     # return day_list
#
#
# # 给定起始结束日期来获取数据
# def quotes_history(maxThread, begin_date, end_date):
#     # day_list = DateTool.getBetweenDayList(begin_date, end_date, format="%Y%m%d")
#     day_list = getTradeCal(startDate=begin_date, endDate=end_date, type='1')
#     # 开始多线程, 设置线程数
#     for i in range(maxThread):
#         t = threading.Thread(target=quotes_history_thread)
#         t.daemon = True
#         t.start()
#
#     # 遍历日期集合添加到queue中
#     for qu in day_list:
#         queue_history.put(qu)
#     queue_history.join()
#
#
# def quotes_history_thread():
#     while True:
#         i = queue_history.get()
#         # 多线程轮询访问
#         # 日线行情
#         quotes_e.quotes_daily(trade_date=i)
#         # 复权因子
#         quotes_e.adj_trade(trade_date=i)
#         # 每日指标
#         quotes_e.get_daily_basic(trade_date=i)
#
#
# def today():
#     quotesDataAll = basic(engine, pro, logger)
#     # 获取当前股票列表
#     quotesDataAll.stock_basic()
#     # 获取公司信息
#     quotesDataAll.stock_company()
#     # 获取名称变更
#     quotesDataAll.name_change()
#     # 获取沪深股通成份股
#     quotesDataAll.hs_const(hs_type='SH')
#     quotesDataAll.hs_const(hs_type='SZ')
#     #
#     quotesDataAll.new_share(start_date='20100101', end_date='20180101')
#
#
# # getHistory(60,'20060101','20181101')
#
# # 清空表
# # def cleanTable(engine, logger, name):
# # 清空basic_stock表，存放当天数据, 在没有数据返回的时候会报错，
# # try:
# #     pd.read_sql_query("TRUNCATE " + name, con = engine)
# # except ResourceClosedError as e:
# #     logger.info(e)
#
#
# # 多线程
# def threadFuncion(maxThread, begin_date, end_date, claschildThreadName):
#     # day_list = DateTool.getBetweenDayList(begin_date, end_date, format="%Y%m%d")
#     day_list = getTradeCal(startDate=begin_date, endDate=end_date, type='1')
#     # 开始多线程, 设置线程数
#     for i in range(maxThread):
#         t = threading.Thread(target=claschildThreadName)
#         t.daemon = True
#         t.start()
#     # 遍历日期集合添加到queue中
#     for qu in day_list:
#         queue_history.put(qu)
#     queue_history.join()
#
#
# def childThread(classEntity, functionName):
#     while True:
#         i = queue_history.get()
#         # 多线程轮询访问
#         # fundEntity.
#
#
# #  fund 信息
# def fund1():
#     # fundEntity = fund(engine, pro, logger)
#
#     # basic & fundCompany 先删除在获取
#     # cleanTable(engine,logger,"fund_basic")
#     # fundEntity.basic()
#     # cleanTable(engine, logger, "fund_company")
#     # fundEntity.company()
#
#     threadFuncion(16, '20180907', '20181130', 'fundEntity', 'nav')
#     # nav & div 按照日期获取
#     # fundEntity.nav(end_date="20181129")
#     # fundEntity.div(ann_date='20181018')
#     # fundEntity.div(ex_date ='20181018')
#     # fundEntity.div(pay_date='20181018')

def getAll():
    engine, pro, logger = configuration.sql_tuShare_log()
        # 基础数据测试
    # basic_entry = basic.basic(engine, pro, logger)
    # basic_entry.stock_basic(None, None, None)  # 股票列表
    # basic_entry.hs_const("SH", None)  # 沪深股通成份股
    # basic_entry.hs_const("SZ", None)  # 沪深股通成份股
    # basic_entry.stock_company(None, None)  # 上市公司基本信息
    # basic_entry.trade_Cal()  # 交易日历
    market_entry = market_data.makret_data(engine, pro, logger)
    market_entry.daily_cycle(start_date="20180101",end_date="20200531")


getAll()
# fund1()



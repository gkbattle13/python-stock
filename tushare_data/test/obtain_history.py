#!/usr/bin/env python
# -*- coding: utf-8 -*-
# import pandas as pd
import inspect
import os
import sys
import time

from tushare_data import configuration
import pandas as pd

from tushare_data.data.box2 import market_data, basic, financial_statements

current_path = inspect.getfile(inspect.currentframe())
# 获取当前文件所在目录，相当于当前文件的父目录
dir_name = os.path.dirname(current_path)
# 转换为绝对路径
file_abs_path = os.path.abspath(dir_name)
# 划分目录，比如a/b/c划分后变为a/b和c
list_path = os.path.split(file_abs_path)
print("添加包路径为：" + list_path[0])
sys.path.append(list_path[0])


# 行情数据 start 开始时间 end 结束时间 包头不包尾
def quotes_all(engine, pro, logger, start, end):
    market_data_e = market_data.market_data(engine, pro, logger)

    sql = 'select cal_date from basic_trade_cal where is_open = 1 and cal_date >= ' + start + ' and cal_date < ' + end
    data_1 = pd.read_sql(sql, engine)
    for index, row in data_1.iterrows():
        date = row["cal_date"]
        # 港股通每日成交统计  被限制为一分钟两次
        # market_data_e.daily(trade_date=date) # 日线 &  每日指标
        # market_data_e.money_flow(trade_date=date) # 个股资金流向
        # market_data_e.limit_list(trade_date=date)  # 每日涨跌停统计
        # market_data_e.moneyflow_hsgt(trade_date=date) # 沪深港通资金流向
        # market_data_e.hsgt_top10(trade_date=date)  # 沪深股通十大成交股
        # market_data_e.hk_hold(trade_date=date)  # 沪深港股通持股明细
        market_data_e.ggt_daily(trade_date=date) # 港股通每日成交统计 每分钟限制两次
        # market_data_e.bak_daily(trade_date=date) # 备用行情 一天限制500次
        time.sleep(30)


# 基础数据  列表， 上市公司管理，薪酬和持股
def basic_data(engine, pro, logger,start, end):

    # 基础数据获取  列表
    basic_entry = basic.basic(engine, pro, logger)
    basic_entry.stock_basic(None, None, None)  # 股票列表
    # basic_entry.trade_Cal("SSE", None, None)  # 交易日

    # 获取日期，包含法定节假日
    sql = 'select cal_date from basic_trade_cal where cal_date >= ' + start + ' and cal_date < ' + end
    data_1 = pd.read_sql(sql, engine)
    for index, row in data_1.iterrows():
        date = row["cal_date"]
        basic_entry.stk_managers(ann_date=date) # 上市公司管理层
        # basic_entry.stk_rewards(end_date=date) # 薪酬和持股

# 财务数据
def financial_data(engine, pro, logger,start, end):
    fina_entry = financial_statements.financial_statements(engine, pro, logger)
    fina_entry.daily_cycle()
    fina_entry.daily_cycle_quarterly()



# 请求各个分类
def run():
    # 指定配置文件
    engine, pro, logger = configuration.sql_tuShare_log('config-local.ini')

    # basic_data(engine,pro,logger,'20200926',"20201217")
    quotes_all(engine, pro, logger, '20200630',"20200926")

# 运行
run()

#!/usr/bin/env python
# -*- coding: utf-8 -*-
# import pandas as pd
import inspect
import os
import sys
import time

from tushare_data import configuration
import pandas as pd

from tushare_data.data.box2 import market_data, macro_data, financial_statements, basic

current_path = inspect.getfile(inspect.currentframe())
# 获取当前文件所在目录，相当于当前文件的父目录
dir_name = os.path.dirname(current_path)
# 转换为绝对路径
file_abs_path = os.path.abspath(dir_name)
# 划分目录，比如a/b/c划分后变为a/b和c
list_path = os.path.split(file_abs_path)
print("添加包路径为：" + list_path[0])
sys.path.append(list_path[0])


# 基础数据
def base_t(engine, pro, logger):
    basic_entry = basic.basic(engine, pro, logger)
    basic_entry.stock_basic(None, None, None)  # 股票列表
    basic_entry.hs_const("SH", None)  # 沪深股通成份股
    basic_entry.hs_const("SZ", None)  # 沪深股通成份股
    basic_entry.stock_company(None, None)  # 上市公司基本信息
    sql = 'select ts_code from basic_stock where list_status = "L" '
    data_1 = pd.read_sql(sql, engine)
    for index, row in data_1.iterrows():
        date_2 = row["ts_code"]
        basic_entry.stk_managers(ts_code=date_2)  # 上市公司管理层
        # basic_entry.stk_rewards(ts_code=date_2)   # 管理层薪酬和持股
        time.sleep(0.2)


# 日数据  根据交易日历轮询
def quotes_all(engine, pro, logger):
    # 获取交易日历
    sql = 'select cal_date from basic_trade_cal where is_open = 1 and cal_date >= 20190101 and cal_date <= 20191231'
    data_1 = pd.read_sql(sql, engine)

    # 市场数据
    market_data_e = market_data.market_data(engine, pro, logger)

    for index, row in data_1.iterrows():
        date = row["cal_date"]
        # 备用行情 未获取到数据
        # market_data_e.bak_daily(trade_date=date)

        # 日线行情 & 每日指标  获取到20000101 到 20191231
        # market_data_e.daily(trade_date=date)

        # 个股资金走向   以下获取日期为 20100101 到 20191231
        # market_data_e.money_flow(trade_date=date)
        # 沪深港通资金流向
        # market_data_e.moneyflow_hsgt(trade_date=date)
        # 沪深港股通持股明细
        # market_data_e.hk_hold(trade_date=date)


# 财务报表
def finance_all(engine, pro, logger):
    financial_statements_en = financial_statements.financial_statements(engine, pro, logger)

    # 获取股票代码
    sql = 'select ts_code from basic_stock where list_status = "L" '
    data_1 = pd.read_sql(sql, engine)
    for index, row in data_1.iterrows():
        date_2 = row["ts_code"]
        # 循环获取财务报表
        financial_statements_en.financial(ts_code=date_2)


def run(needDate):
    engine, pro, logger = configuration.sql_tuShare_log('config-local.ini')

    # base_t(engine, pro, logger)
    # 行情数据
    # quotes_all(engine, pro, logger)

    # 该接口每天获取上线为500
    # market_data_entry = market_data.market_data(engine, pro, logger)
    # market_data_entry.bak_daily(trade_date="20100101")

    # 财务报表
    f = financial_statements.financial_statements(engine, pro, logger)
    f.daily_cycle()
    f.daily_cycle_quarterly()


run('20200923')

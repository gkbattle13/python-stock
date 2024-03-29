# -*- coding: utf-8 -*-

# 获取基础数据
import inspect
import os
import sys
import time
import pandas as pd
import slowToRich.america.stock.america_stock as america_stock

# 获取当前文件路径
current_path = inspect.getfile(inspect.currentframe())

file_abs_path = os.path.abspath(current_path)
# 划分目录，比如a/b/c划分后变为a/b和c
list_path = os.path.split(file_abs_path)
print("添加包路径为：" + list_path[0])
sys.path.append(list_path[0])

from tushare_data import configuration
from tushare_data.data.box2 import basic, financial_statements
from tushare_data.data.box2 import market_data
from tushare_data.data.box2 import fund
from tushare_data.data.box2 import market_reference_resources


# 基础数据
def base_t(engine, pro, logger, date):
    basic_entry = basic.basic(engine, pro, logger)
    # 每天更新的数据
    basic_entry.stock_basic(None, None, None)  # 股票列表
    basic_entry.hs_const("SH", None)  # 沪深股通成份股
    basic_entry.hs_const("SZ", None)  # 沪深股通成份股
    basic_entry.stock_company(None, None)  # 上市公司基本信息
    # 获取当前文件所在目录，相当于当前文件的父目录
    dir_name = os.path.dirname(current_path)
    # 转换为绝对路径
    # TODO 需要测试当天是否能获取到昨天的数据
    date = "20200926"
    basic_entry.stk_managers(ann_date=date)  # 上市公司管理层
    basic_entry.stk_rewards(ts_code=date)  # 管理层薪酬和持股


# 行情数据
def market_t(engine, pro, logger, needDate):
    # needDate = "20200520"
    market_entry = market_data.makret_data(engine, pro, logger)
    market_entry.daily(trade_date=needDate)  # 日线
    market_entry.daily_basic(trade_date=needDate)  # 每日指标
    market_entry.money_flow(trade_date=needDate)  # 个股资金流向
    market_entry.moneyflow_hsgt(trade_date=needDate)  # 沪深港通资金流向
    market_entry.hsgt_top10(trade_date=needDate)  # 沪深股通十大成交股
    market_entry.hk_hold(trade_date=needDate)  # 沪深港股通持股明细
    market_entry.ggt_daily(trade_date=needDate)  # 港股通每日成交统计
    market_entry.index_global(trade_date=needDate)  # 国际指数


# 市场产考信息
def reference_t(engine, pro, logger, needDate):
    # needDate = "20200520"
    reference_entry = market_reference_resources.market_reference_resources(engine, pro, logger)
    reference_entry.stk_holdertrade(ann_date=needDate)  # 股东增减持
    reference_entry.ggt_top10(trade_date=needDate)  # 港股通十大成交股
    reference_entry.margin(trade_date=needDate)  # 融资融券交易汇总
    reference_entry.margin_detail(trade_date=needDate)  # 融资融券交易明细
    reference_entry.top_list(trade_date=needDate)  # 龙虎榜每日明细
    reference_entry.top_inst(trade_date=needDate)  # 龙虎榜机构明细
    reference_entry.repurchase(ann_date=needDate)  # 股票回购
    reference_entry.concept()  # 概念股分类
    reference_entry.concept_detail()  # 概念股列表
    reference_entry.share_float(ann_date=needDate)  # 限售股解禁

    # reference_entry.ggt_top10(trade_date="20200520")
    # reference_entry.margin_detail(trade_date="20200520")
    # reference_entry.top_list(trade_date="20200520")
    # reference_entry.top_inst(trade_date="20200520")
    # reference_entry.share_float(ann_date='20181220')
    # reference_entry.block_trade(trade_date='20200519') # 大宗交易，好像只能获取前天的
    # reference_entry.stk_account() # 股票账户开户数据统计周期为一周


# 财务数据
def financial_statements_t(engine, pro, logger, needDate):
    f = financial_statements.financial_statements(engine, pro, logger)
    f.financial(ann_date=needDate)


# 基金数据
def fund_t(engine, pro, logger):
    fund_entry = fund.fund(engine, pro, logger)


def run():
    # TODO 处理需要处理的数据
    time_start = time.time()

    needDate = time.strftime("%Y%m%d", time.localtime())
    engine, pro, logger = configuration.sql_tuShare_log('config-local.ini')
    us_stock = america_stock.america_stock(engine, pro, logger)


    # base_t(engine, pro, logger)
    # market_t(engine, pro, logger, needDate)
    # reference_t(engine, pro, logger, needDate)
    # financial_statements_t(engine, pro, logger, needDate)

    # us_stock.article_epu_index()
    # us_stock.weibo_index()
    # us_stock.baidu()
    # us_stock.google()

    # 获取美股名单
    # us_stock.get_us_stock_name()
    # 获取美股知名数据
    # us_stock.stock_us_famous_spot_em()

    # us_stock.get_stock_us_daily()

    # 多线程获取美股数据
    us_stock.get_stock_us_daily_thread()
    time_end = time.time()
    logger.info("f-clock 运行完成共用时：" + str(time_end - time_start) + 's')


run()

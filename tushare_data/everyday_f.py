# 获取基础数据
import os
import sys
import time

sysa = os.path.abspath('..')
sys.path.append(sysa)
from tushare_data import configuration
from tushare_data.data.box2 import basic
from tushare_data.data.box2 import fund
from tushare_data.data.box2 import market_data


def base(engine, pro, logger):
    basic_entry = basic.basic(engine, pro, logger)
    basic_entry.stock_basic(None, None, None)


def makret_t(engine, pro, logger):
    market_entry = market_data.makret_data(engine, pro, logger)
    market_entry.daily(trade_date=time.strftime("%Y%m%d", time.localtime()))


def fund_t(engine, pro, logger):
    fund_entry = fund.fund(engine, pro, logger)


def run():
    engine, pro, logger = configuration.sql_tuShare_log()
    # base(engine, pro, logger)
    makret_t(engine, pro, logger)
    fund_t(engine, pro, logger)


run()

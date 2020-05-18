# 获取基础数据
import os
import sys
sysa = os.path.abspath('..')
sys.path.append(sysa)
from tushare_data import configuration
from tushare_data.data.box2 import basic

def base(engine, pro, logger):
    basic_entry = basic.basic(engine, pro, logger)
    basic_entry.stock_basic(None, None, None)

def run():
    engine, pro, logger = configuration.sql_tuShare_log()
    base(engine, pro, logger)

run()

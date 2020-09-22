from datetime import time

from tushare_data import configuration
from tushare_data.data.box2 import fund



# 基础数据
def fund_t(engine, pro, logger):
    fund_entry = fund.fund(engine, pro, logger)
    # fund_entry.basic()
    fund_entry.portfolio("1500211.SZ")  # 基金持股变化


def run():
    engine, pro, logger = configuration.sql_tuShare_log()
    fund_t(engine,pro,logger)

run()
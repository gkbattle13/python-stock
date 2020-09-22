
# 基础数据
from tushare_data import configuration
from tushare_data.data.box2 import alternative
from tushare_data.utils import tushare_util


def fund_t(engine, pro, logger):
    fund_entry = alternative.alternative(engine, pro, logger)
    # fund_entry.basic()
    # fund_entry.news_cycle(start_date="20200101", end_date="20200723",   src="10jqka")  # 基金持股变化
    fund_entry.news(start_date="2020-07-24 00:00:00", end_date="2020-07-24 18:00:00",   src="10jqka")  # 基金持股变化

def fund_t_one(engine, pro, logger):
    tushare_util_entry = tushare_util.tushare_util(engine, pro, logger)
    tushare_util_entry.common_interface(fun_name='stock_basic',table_name = "test_1",info="table_name",filed= "",  ts_code ='000001.SZ')  # 基金持股变化


def run():
    engine, pro, logger = configuration.sql_tuShare_log()
    fund_t(engine,pro,logger)
    # fund_t_one(engine,pro,logger)

run()
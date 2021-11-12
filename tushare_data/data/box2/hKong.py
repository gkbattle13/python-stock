#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sqlalchemy
import tushare as ts
from tushare_data.utils import strUtils


# 获取行情数据
class hKong():

    # 初始化: 数据连接：(engine), tushare api：(pro), 日志：（logger）
    def __init__(self, engine, pro, logger):
        self.engine = engine
        self.pro = pro
        self.logger = logger
        logger.info("sqlalchemy版本为：" + sqlalchemy.__version__ + "。 ts 版本：" + ts.__version__)  # 查看版本\
        # reload(sys)
        # sys.setdefaultencoding('utf-8')

    # 获取交易日
    def hk_tradecal(self, start_date=None, end_date=None, is_open=None):
        full_name = "TuShare 香港 港股交易日历 hk_tradecal"
        parameter = str(
            {'start_date': strUtils.noneToUndecided(start_date), 'is_open': strUtils.noneToUndecided(is_open),
             'end_date': strUtils.noneToUndecided(end_date)})
        try:
            # 交易日
            data = self.pro.hk_tradecal(start_date=start_date, end_date=end_date, is_open=is_open)
            data.to_sql("hk_tradecal", self.engine, if_exists="append", index=False)

            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="tradecal,HK", parameter=parameter,
                                  status=1, result_count=str(len(data)))
        except Exception as e:
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="tradecal,HK", parameter=parameter,
                                  status=0, error_info=str(e), result_count=None)

    # 获取
    def hk_daily(self, start_date=None, end_date=None, ts_code=None, trade_date=None):
        full_name = "TuShare 香港 港股行情 hk_daily"
        parameter = str(
            {'start_date': strUtils.noneToUndecided(start_date), 'ts_code': strUtils.noneToUndecided(ts_code),
             'end_date': strUtils.noneToUndecided(end_date), 'trade_date': strUtils.noneToUndecided(trade_date)})
        try:
            # 交易日
            data = self.pro.hk_daily(start_date=start_date, end_date=end_date, ts_code=ts_code, trade_date=trade_date)
            data.to_sql("hk_daily", self.engine, if_exists="append", index=False)

            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="daily,HK", parameter=parameter,
                                  status=1, result_count=str(len(data)))
        except Exception as e:
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="daily,HK", parameter=parameter,
                                  status=0, error_info=str(e), result_count=None)

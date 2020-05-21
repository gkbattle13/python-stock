#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sqlalchemy
from datetime import time
import pandas as pd
import time
import tushare as ts
from tushare_data.utils import strUtils
from tushare_data.utils.date import date_tool


# 获取行情数据
class makret_data():

    # 初始化: 数据连接：(engine), tushare api：(pro), 日志：（logger）
    def __init__(self, engine, pro, logger):
        self.engine = engine
        self.pro = pro
        self.logger = logger
        logger.info("sqlalchemy版本为：" + sqlalchemy.__version__ + "。 ts 版本：" + ts.__version__)  # 查看版本\
        # reload(sys)
        # sys.setdefaultencoding('utf-8')

    """
    根据输入的日期循环获取一段时间内日线行情的数据 循环调用daily接口,使用trade_date参数，可能有些股票现在不存在
    """

    def daily_cycle(self, exchange=None, start_date=None, end_date=None):
        sql = 'select * from basic_trade_cal where is_open = 1 '
        if not exchange is None:
            sql = sql + ' and exchange = "' + exchange + '"'
        if not start_date is None:
            sql = sql + ' and cal_date > ' + start_date
        if not end_date is None:
            sql = sql + ' and cal_date < ' + end_date
        data_1 = pd.read_sql(sql, self.engine)
        for index, row in data_1.iterrows():
            date_2 = row["cal_date"]
            date_2.decode('utf8')
            self.daily(trade_date=date_2)

    """
    根据输入的日期循环获取一段时间内沪深港通资金流向 循环调用moneyflow_hsgt接口,使用trade_date参数，可能有些股票现在不存在
    """

    def ggt_daily_cycle(self, exchange=None, start_date=None, end_date=None, sleep=None):
        sql = 'select * from basic_trade_cal where is_open = 1 '
        if not exchange is None:
            sql = sql + ' and exchange = "' + exchange + '"'
        if not start_date is None:
            sql = sql + ' and cal_date > ' + start_date
        if not end_date is None:
            sql = sql + ' and cal_date < ' + end_date
        data_1 = pd.read_sql(sql, self.engine)
        for index, row in data_1.iterrows():
            date_2 = row["cal_date"]
            date_2.decode('utf8')
            self.ggt_daily(trade_date=date_2)
            if not sleep is None:
                time.sleep(sleep)

    """
    根据输入的日期循环获取一段时间内个股资金流向 循环调用money_flow接口,使用trade_date参数，可能有些股票现在不存在
    """

    def money_flow_cycle(self, exchange=None, start_date=None, end_date=None):
        sql = 'select * from basic_trade_cal where is_open = 1 '
        if not exchange is None:
            sql = sql + ' and exchange = "' + exchange + '"'
        if not start_date is None:
            sql = sql + ' and cal_date > ' + start_date
        if not end_date is None:
            sql = sql + ' and cal_date < ' + end_date
        data_1 = pd.read_sql(sql, self.engine)
        for index, row in data_1.iterrows():
            date_2 = row["cal_date"]
            date_2.decode('utf8')
            self.money_flow(trade_date=date_2)


    """
    根据输入的日期循环获取一段时间内沪深股通十大成交股 循环调用hsgt_top10接口,使用trade_date参数，可能有些股票现在不存在
    """

    def hsgt_top10_cycle(self, exchange=None, start_date=None, end_date=None, sleep=None):
        sql = 'select * from basic_trade_cal where is_open = 1 '
        if not exchange is None:
            sql = sql + ' and exchange = "' + exchange + '"'
        if not start_date is None:
            sql = sql + ' and cal_date > ' + start_date
        if not end_date is None:
            sql = sql + ' and cal_date < ' + end_date
        data_1 = pd.read_sql(sql, self.engine)
        for index, row in data_1.iterrows():
            date_2 = row["cal_date"]
            date_2.decode('utf8')
            self.hsgt_top10(trade_date=date_2)
            if not sleep is None:
                time.sleep(sleep)

    """
    根据输入的日期循环获取一段时间内沪深港通资金流向 循环调用moneyflow_hsgt接口,使用trade_date参数，可能有些股票现在不存在
    """

    def moneyflow_hsgt_cycle(self, exchange=None, start_date=None, end_date=None):
        sql = 'select * from basic_trade_cal where is_open = 1 '
        if not exchange is None:
            sql = sql + ' and exchange = "' + exchange + '"'
        if not start_date is None:
            sql = sql + ' and cal_date > ' + start_date
        if not end_date is None:
            sql = sql + ' and cal_date < ' + end_date
        data_1 = pd.read_sql(sql, self.engine)
        for index, row in data_1.iterrows():
            date_2 = row["cal_date"]
            date_2.decode('utf8')
            self.moneyflow_hsgt(trade_date=date_2)


    """
        根据输入的时间循环获取一段时间内的数据 循环调用weekly接口,使用trade_date参数，可能有些股票现在不存在
    """

    def weekly_cycle(self, exchange=None, start_date=None, end_date=None):
        sql = 'select * from basic_trade_cal where is_open = 1 '
        if not exchange is None:
            sql = sql + ' and exchange = "' + exchange + '"'
        if not start_date is None:
            sql = sql + ' and cal_date > ' + start_date
        if not end_date is None:
            sql = sql + ' and cal_date < ' + end_date

        data_1 = pd.read_sql(sql, self.engine)
        for index, row in data_1.iterrows():
            date_2 = row["cal_date"]
            date_2.decode('utf8')
            week = date_tool.changeToWeek(date_2)
            if week == 5:
                self.weekly(trade_date=date_2)

    """
        循环获取一段时间内的数据 循环调用monthly接口,使用trade_date参数，可能有些股票现在不存在
    """

    def monthly_cycle(self, exchange=None, start_date=None, end_date=None):
        # TODO 需要替换为获取每周第一个日期和每月第一个日期
        sql = 'select * from basic_trade_cal where is_open = 1 '
        if not exchange is None:
            sql = sql + ' and exchange = "' + exchange + '"'
        if not start_date is None:
            sql = sql + ' and cal_date > ' + start_date
        if not end_date is None:
            sql = sql + ' and cal_date < ' + end_date
        data_1 = pd.read_sql(sql, self.engine)
        # data_1 = date_tool.getLastDayOfMonth(start_date,end_date)
        for index, row in data_1.iterrows():
            date_2 = row["cal_date"]
            date_2.decode('utf8')
            self.monthly(trade_date=date_2)
            time.sleep(0.4)

    """
    日线行情
    接口：daily
    更新时间：交易日每天15点～16点之间
    描述：获取股票行情数据，或通过通用行情接口获取数据，包含了前后复权数据．
    ts_code	str	N	股票代码（二选一）
    trade_date	str	N	交易日期（二选一）
    start_date	str	N	开始日期(YYYYMMDD)
    end_date	str	N	结束日期(YYYYMMDD)

    """

    def daily(self, ts_code=None, trade_date=None, start_date=None, end_date=None):
        full_name = "TuShare 行情数据 日线行情 daily"
        parameter = str(
            {'ts_code': strUtils.noneToUndecided(ts_code), 'trade_date': strUtils.noneToUndecided(trade_date),
             'start_date': strUtils.noneToUndecided(start_date), 'end_date': strUtils.noneToUndecided(end_date)})
        try:
            data = self.pro.daily(ts_code=ts_code, trade_date=trade_date, start_date=start_date, end_date=end_date)
            data.to_sql("makret_data_daily", self.engine, if_exists="append", index=False)
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="daily", parameter=parameter,
                                  status=1, result_count=str(len(data)))
        except Exception as e:
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="daily", parameter=parameter,
                                  status=0, error_info=str(e), result_count=None)

    """
        周线行情
        接口：weekly
        描述：获取A股周线行情
        ts_code	str	N	TS代码 （ts_code,trade_date两个参数任选一）
        trade_date	str	N	交易日期 （每周最后一个交易日期，YYYYMMDD格式）
        start_date	str	N	开始日期
        end_date	str	N	结束日期

       """

    def weekly(self, ts_code=None, trade_date=None, start_date=None, end_date=None):
        data = self.pro.weekly(ts_code=ts_code, trade_date=trade_date, start_date=start_date, end_date=end_date)
        self.logger.info('TuShare weekly 周线行情 ts_code: ' + strUtils.noneToUndecided(ts_code) + " trade_date： " +
                         strUtils.noneToUndecided(trade_date) + " start_date： " +
                         strUtils.noneToUndecided(start_date) + " end_date： " +
                         strUtils.noneToUndecided(end_date) + ' 数据共：' + str(len(data)) + "条数据")
        data.insert(11, 'create_date', str(time.strftime("%Y-%m-%d", time.localtime())))
        data.to_sql("makret_data_weekly", self.engine, if_exists="append", index=False)


    """
        月线行情
        接口：monthly
        描述：获取A股月线行情
        ts_code	str	N	TS代码 （ts_code,trade_date两个参数任选一）
        trade_date	str	N	交易日期 （每周最后一个交易日期，YYYYMMDD格式）
        start_date	str	N	开始日期
        end_date	str	N	结束日期
    """

    def monthly(self, ts_code=None, trade_date=None, start_date=None, end_date=None):
        data = self.pro.monthly(ts_code=ts_code, trade_date=trade_date, start_date=start_date, end_date=end_date)
        self.logger.info('TuShare monthly 月线 ts_code: ' + strUtils.noneToUndecided(ts_code) + " trade_date： " +
                         strUtils.noneToUndecided(trade_date) + " start_date： " +
                         strUtils.noneToUndecided(start_date) + " end_date： " +
                         strUtils.noneToUndecided(end_date) + ' 数据共：' + str(len(data)) + "条数据")
        data.insert(11, 'create_date', str(time.strftime("%Y-%m-%d", time.localtime())))
        data.to_sql("makret_data_monthly", self.engine, if_exists="append", index=False)

    def quotes_daily(self, ts_code=None, trade_date=None, start_date=None, end_date=None):
        try:
            data = self.pro.daily(ts_code=ts_code, trade_date=trade_date, start_date=start_date, end_date=end_date)
            data.to_sql("quotes_daily", self.engine, if_exists="append", index=False)
            self.logger.info("TuShare 获取行情数据成功：ts_code：" + strUtils.noneToUndecided(ts_code) + ", 交易日期：" +
                             strUtils.noneToUndecided(trade_date) + ",  开始日期： " + strUtils.noneToUndecided(start_date) +
                             ", 结束日期：  " + strUtils.noneToUndecided(end_date) + " ,  数据量： " + str(len(data)) + "成功")
        except Exception as e:
            self.logger.errorlog(fun_name='daily', ts_code=ts_code, trade_date=trade_date, start_date=start_date,
                                 end_date=end_date, e=str(e))
            self.logger.error("TuShare 获取行情数据失败：code：" + strUtils.noneToUndecided(ts_code) + ", 交易日期：" +
                              strUtils.noneToUndecided(trade_date) + "  开始日期： " + strUtils.noneToUndecided(start_date) +
                              ", 结束日期：  " + strUtils.noneToUndecided(end_date) + " , 失败，错误详情： " + str(e))

    """
    接口：每日指标 daily_basic
    更新时间：交易日每日15点～17点之间
    描述：获取全部股票每日重要的行情数据，可用于选股分析、报表展示等。
    ts_code	str	Y	股票代码（二选一）
    trade_date	str	N	交易日期 （二选一）
    start_date	str	N	开始日期(YYYYMMDD)
    end_date	str	N	结束日期(YYYYMMDD)
    """

    # 根据参数获取每日指标
    def daily_basic(self, ts_code=None, trade_date=None, start_date=None, end_date=None):
        full_name = "TuShare 行情数据 每日指标 daily_basic"
        parameter = str(
            {'ts_code': strUtils.noneToUndecided(ts_code), 'trade_date': strUtils.noneToUndecided(trade_date),
             'start_date': strUtils.noneToUndecided(start_date), 'end_date': strUtils.noneToUndecided(end_date)})
        try:
            data = self.pro.daily_basic(ts_code=ts_code, trade_date=trade_date, start_date=start_date,
                                        end_date=end_date)
            data.to_sql("makret_daily_basic", self.engine, if_exists="append", index=False)
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="daily_basic", parameter=parameter,
                                  status=1, result_count=str(len(data)))
        except Exception as e:
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="daily_basic", parameter=parameter,
                                  status=0, error_info=str(e), result_count=None)

    """
    接口：每日涨跌停统计 limit_list
    描述：获取每日涨跌停股票统计，包括封闭时间和打开次数等数据，帮助用户快速定位近期强（弱）势股，以及研究超短线策略。
    限量：单次最大1000，总量不限制
    trade_date	str	N	交易日期 YYYYMMDD格式，支持单个或多日期输入
    ts_code	str	N	股票代码 （支持单个或多个股票输入）
    limit_type	str	N	涨跌停类型：U涨停D跌停
    start_date	str	N	开始日期 YYYYMMDD格式
    end_date	str	N	结束日期 YYYYMMDD格式
    """

    # 根据参数获取每日指标
    def limit_list(self, ts_code=None, trade_date=None, start_date=None, end_date=None, limit_type=None):
        # TODO 可以统计某一段时间内某一板块涨停状态
        try:
            data = self.pro.limit_list(ts_code=ts_code, limit_type=limit_type, trade_date=trade_date,
                                       start_date=start_date,
                                       end_date=end_date)
            data.to_sql("makret_limit_list", self.engine, if_exists="append", index=False)
            self.logger.info("TuShare 获取每日涨跌停统计成功：ts_code：" + strUtils.noneToUndecided(ts_code) + ", 交易日期：" +
                             strUtils.noneToUndecided(trade_date) + ",  开始日期： " + strUtils.noneToUndecided(start_date) +
                             ", 结束日期：  " + strUtils.noneToUndecided(end_date) + " ,  数据量： " + str(len(data)) + "成功")
        except Exception as e:
            self.logger.errorlog(fun_name='limit_list', ts_code=ts_code, trade_date=trade_date, start_date=start_date,
                                 end_date=end_date, e=str(e))
            self.logger.error("TuShare 获取行情数据失败：code：" + strUtils.noneToUndecided(ts_code) + ", 交易日期：" +
                              strUtils.noneToUndecided(trade_date) + "  开始日期： " + strUtils.noneToUndecided(start_date) +
                              ", 结束日期：  " + strUtils.noneToUndecided(end_date) + " , 失败，错误详情： " + str(e))



    """
    接口：沪深港通资金流向 moneyflow_hsgt
    描述：获取沪股通、深股通、港股通每日资金流向数据，每次最多返回300条记录，总量不限制。
    trade_date	str	N	交易日期 (二选一)
    start_date	str	N	开始日期 (二选一)
    end_date	str	N	结束日期
    """

    # 沪深港通资金流向
    def moneyflow_hsgt(self, trade_date=None, start_date=None, end_date=None):
        full_name = "TuShare 行情数据 沪深港通资金流向 moneyflow_hsgt"
        parameter = str(
            {'trade_date': strUtils.noneToUndecided(trade_date),
             'start_date': strUtils.noneToUndecided(start_date), 'end_date': strUtils.noneToUndecided(end_date)})
        try:
            data = self.pro.moneyflow_hsgt(trade_date=trade_date, start_date=start_date, end_date=end_date)
            data.to_sql("makret_moneyflow_hsgt", self.engine, if_exists="append", index=False)
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="moneyflow_hsgt",
                                  parameter=parameter,
                                  status=1, error_info=None, result_count=str(len(data)))
        except Exception as e:
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="moneyflow_hsgt",
                                  parameter=parameter,
                                  status=0, error_info=str(e), result_count=None)

    """
    接口：沪深股通十大成交股 hsgt_top10
    描述：获取沪股通、深股通每日前十大成交详细数据
    ts_code	str	N	股票代码（二选一）
    trade_date	str	N	交易日期（二选一）
    start_date	str	N	开始日期
    end_date	str	N	结束日期
    market_type	str	N	市场类型（1：沪市 3：深市）
    """

    # 沪深港通资金流向
    def hsgt_top10(self, ts_code=None, trade_date=None, start_date=None, end_date=None, market_type=None):
        full_name = "TuShare 行情数据 沪深股通十大成交股 hsgt_top10"
        parameter = str(
            {'ts_code': strUtils.noneToUndecided(ts_code), 'trade_date': strUtils.noneToUndecided(trade_date),
             'start_date': strUtils.noneToUndecided(start_date), 'end_date': strUtils.noneToUndecided(end_date),
             'market_type': strUtils.noneToUndecided(market_type)})
        try:
            data = self.pro.hsgt_top10(ts_code=ts_code, trade_date=trade_date, start_date=start_date, end_date=end_date,
                                       market_type=market_type)
            data.to_sql("makret_hsgt_top10", self.engine, if_exists="append", index=False)
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="hsgt_top10",
                                  parameter=parameter,
                                  status=1, error_info=None, result_count=str(len(data)))
        except Exception as e:
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="hsgt_top10",
                                  parameter=parameter,
                                  status=0, error_info=str(e), result_count=None)

    """
        个股资金流向
        接口：moneyflow
        描述：获取沪深A股票资金流向数据，分析大单小单成交情况，用于判别资金动向
        ts_code	str	N	股票代码 （股票和时间参数至少输入一个）
        trade_date	str	N	交易日期
        start_date	str	N	开始日期
        end_date	str	N	结束日期
    """

    def money_flow(self, ts_code=None, trade_date=None, start_date=None, end_date=None):
        full_name = "TuShare 行情数据 个股资金流向 money_flow"
        parameter = str(
            {'ts_code': strUtils.noneToUndecided(ts_code), 'trade_date': strUtils.noneToUndecided(trade_date),
             'start_date': strUtils.noneToUndecided(start_date), 'end_date': strUtils.noneToUndecided(end_date)})
        try:
            data = self.pro.moneyflow(ts_code=None, trade_date=trade_date, start_date=start_date, end_date=end_date)
            data.to_sql("makret_money_flow", self.engine, if_exists="append", index=False)
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="money_flow", parameter=parameter,
                                  status=1, error_info=None, result_count=str(len(data)))
        except Exception as e:
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="money_flow", parameter=parameter,
                                  status=0, error_info=str(e), result_count=None)

    """
        沪深港股通持股明细
        接口：hk_hold
        描述：获取沪深港股通持股明细，数据来源港交所
        code	str	N	交易所代码
        ts_code	str	N	TS股票代码
        trade_date	str	N	交易日期
        start_date	str	N	开始日期
        end_date	str	N	结束日期
        exchange	str	N	类型：SH沪股通（北向）SZ深股通（北向）HK港股通（南向持股）
    """

    def hk_hold(self, code=None, ts_code=None, trade_date=None, start_date=None, end_date=None, exchange=None):
        full_name = "TuShare 行情数据 沪深港股通持股明细 hk_hold"
        parameter = str(
            {'ts_code': strUtils.noneToUndecided(ts_code), 'trade_date': strUtils.noneToUndecided(trade_date),
             'start_date': strUtils.noneToUndecided(start_date), 'end_date': strUtils.noneToUndecided(end_date)})
        try:
            data = self.pro.hk_hold(code=None, ts_code=None, trade_date=trade_date, start_date=start_date,
                                    end_date=end_date, exchange=None)
            data.to_sql("makret_hk_hold", self.engine, if_exists="append", index=False)
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="hk_hold",
                                  parameter=parameter,
                                  status=1, error_info=None, result_count=str(len(data)))
        except Exception as e:
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="hk_hold",
                                  parameter=parameter,
                                  status=0, error_info=str(e), result_count=None)


    """
    接口：港股通每日成交统计 ggt_daily
    描述：获取港股通每日成交信息，数据从2014年开始
    trade_date	str	N	交易日期 （格式YYYYMMDD，下同。支持单日和多日输入）
    start_date	str	N	开始日期
    end_date	str	N	结束日期
    """

    # 港股通每日成交统计
    def ggt_daily(self, trade_date=None, start_date=None, end_date=None):
        full_name = "TuShare 行情数据 港股通每日成交统计 ggt_daily"
        parameter = str(
            { 'trade_date': strUtils.noneToUndecided(trade_date),
             'start_date': strUtils.noneToUndecided(start_date), 'end_date': strUtils.noneToUndecided(end_date)})
        try:
            data = self.pro.ggt_daily(trade_date=trade_date, start_date=start_date, end_date=end_date)
            data.to_sql("makret_ggt_daily", self.engine, if_exists="append", index=False)
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="ggt_daily",
                              parameter=parameter,
                              status=1, error_info=None, result_count=str(len(data)))
        except Exception as e:
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="ggt_daily",
                              parameter=parameter,
                              status=0, error_info=str(e), result_count=None)

    """
    接口：国际指数 index_global
    描述：获取国际主要指数日线行情 单次最大提取4000行情数据，可循环获取，总量不限制
    trade_date	str	N	交易日期 （格式YYYYMMDD，下同。支持单日和多日输入）
    start_date	str	N	开始日期
    end_date	str	N	结束日期
    """

    # 国际指数
    def index_global(self, trade_date=None, start_date=None, end_date=None):
        full_name = "TuShare 行情数据 港股通每日成交统计 index_global"
        parameter = str(
            { 'trade_date': strUtils.noneToUndecided(trade_date),
             'start_date': strUtils.noneToUndecided(start_date), 'end_date': strUtils.noneToUndecided(end_date)})
        try:
            data = self.pro.index_global(trade_date=trade_date, start_date=start_date, end_date=end_date)
            data.to_sql("makret_index_global", self.engine, if_exists="append", index=False)
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="index_global",
                              parameter=parameter,
                              status=1, error_info=None, result_count=str(len(data)))
        except Exception as e:
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="index_global",
                              parameter=parameter,
                              status=0, error_info=str(e), result_count=None)







    """
    更新时间：早上9点30分
    描述：获取股票复权因子，可提取单只股票全部历史复权因子，也可以提取单日全部股票的复权因子。
    ts_code	str	Y	股票代码
    trade_date	str	N	交易日期(YYYYMMDD，下同)
    start_date	str	N	开始日期
    end_date	str	N	结束日期
    #提取2018年7月18日复权因子
    df = pro.adj_factor(ts_code='', trade_date='20180818')
    """

    def adj_trade(self, ts_code=None, trade_date=None, start_date=None, end_date=None):
        try:
            data = self.pro.adj_factor(ts_code=ts_code, trade_date=trade_date, start_date=start_date, end_date=end_date)
            data.to_sql("quotes_adj_factor", self.engine, if_exists="append", index=False)
            self.logger.info(
                "TuShare 获取复权因子成功" + "日期：" + strUtils.noneToUndecided(trade_date) + ",数据量： " + str(len(data)) + "成功")
        except Exception as e:
            self.logger.errorlog(fun_name='adj_trade', trade_date=trade_date, start_date=start_date, end_date=end_date,
                                 e=str(e))
            self.logger.error("TuShare 获取复权因子失败，日期：" + trade_date + "获取数据失败，保存入errorlog  " + str(e))

    """
    接口：suspend
    更新时间：不定期
    描述：获取股票每日停复牌信息
    ts_code	        str	N	股票代码(三选一)
    suspend_date	str	N	停牌日期(三选一)
    resume_date	    str	N	复牌日期(三选一)
    """

    def suspend(self, ts_code=None, suspend_date=None, resume_date=None):
        try:
            data = self.pro.suspend(ts_code=ts_code, suspend_date=suspend_date, resume_date=resume_date)
            data.to_sql("quotes_suspend", self.engine, if_exists="append", index=False)
            self.logger.info("TuShare 获取停复牌信息成功，ts_code：" + strUtils.noneToUndecided(ts_code) + "停盘日期：" +
                             strUtils.noneToUndecided(suspend_date) + "复牌日期: " + strUtils.noneToUndecided(resume_date) +
                             ", 数据量：" + str(len(data)))
        except Exception as e:
            self.logger.error(fun_name='suspend', ts_code=ts_code, start_date=suspend_date, end_date=resume_date,
                              e=str(e))
            self.logger.error(
                "TuShare 获取停复牌信息失败，ts_code：" + strUtils.noneToUndecided(ts_code) + "停盘日期：" +
                strUtils.noneToUndecided(suspend_date) + "复牌日期: " + strUtils.noneToUndecided(
                    resume_date) + ", 数据获取失败" + str(e))

    """

    接口名称：pro_bar
    更新时间：股票和指数通常在15点～17点之间，数字货币实时更新，具体请参考各接口文档明细。
    描述：目前整合了股票（未复权、前复权、后复权）、指数、数字货币的行情数据，未来还将整合包括期货期权、基金、外汇在内的所有交易行情数据，同时提供分钟数据。
    ts_code	str	Y	证券代码
    start_date	str	N	开始日期 (格式：YYYYMMDD)
    end_date	str	N	结束日期 (格式：YYYYMMDD)
    asset	str	Y	资产类别：E股票 I沪深指数 C数字货币 F期货 O期权，默认E
    adj	str	N	复权类型(只针对股票)：None未复权 qfq前复权 hfq后复权 , 默认None
    freq	str	Y	数据频度 ：1MIN表示1分钟（1/5/15/30/60分钟） D日线 ，默认D
    ma	list	N	均线，支持任意合理int数值
    """

    # 通用行情接口
    #  TODO  暂时为开发
    def pro_bar(self, ts_code=None, trade_date=None, start_date=None, end_date=None):
        try:
            data = self.pro.pro_bar(ts_code=ts_code, trade_date=trade_date, start_date=start_date, end_date=end_date)
            data.to_sql("quotes_daily_basic", self.engine, if_exists="append", index=False)
            self.logger.info("TuShare 获取行情数据成功：ts_code：" + strUtils.noneToUndecided(ts_code) + ", 交易日期：" +
                             strUtils.noneToUndecided(trade_date) + ",  开始日期： " + strUtils.noneToUndecided(start_date) +
                             ", 结束日期：  " + strUtils.noneToUndecided(end_date) + " ,  数据量： " + str(len(data)) + "成功")
        except Exception as e:
            self.logger.errorlog(fun_name='daily_basic', ts_code=ts_code, trade_date=trade_date, start_date=start_date,
                                 end_date=end_date, e=str(e))
            self.logger.error("TuShare 获取行情数据失败：code：" + strUtils.noneToUndecided(ts_code) + ", 交易日期：" +
                              strUtils.noneToUndecided(trade_date) + "  开始日期： " + strUtils.noneToUndecided(start_date) +
                              ", 结束日期：  " + strUtils.noneToUndecided(end_date) + " , 失败，错误详情： " + str(e))

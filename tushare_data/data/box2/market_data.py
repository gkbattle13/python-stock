#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sqlalchemy
from datetime import time
import pandas as pd
import time
import tushare as ts
from sqlalchemy.exc import ResourceClosedError
from tushare_data.utils import strUtils
from tushare_data.utils.date import date_tool
import sys


# 获取行情数据
class makret_data():

    # 初始化: 数据连接：(engine), tushare api：(pro), 日志：（logger）
    def __init__(self, engine, pro, logger):
        self.engine = engine
        self.pro = pro
        self.logger = logger
        logger.info("sqlalchemy版本为：" + sqlalchemy.__version__ + "。 ts 版本：" + ts.__version__)  # 查看版本\
        reload(sys)
        sys.setdefaultencoding('utf-8')

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
        data = self.pro.daily(ts_code=ts_code, trade_date=trade_date, start_date=start_date, end_date=end_date)
        self.logger.info('TuShare daily 日线行情 ts_code: ' + strUtils.noneToWdy(ts_code) + " trade_date： " +
                         strUtils.noneToWdy(trade_date) + " start_date： " +
                         strUtils.noneToWdy(start_date) + " end_date： " +
                         strUtils.noneToWdy(end_date) + ' 数据共：' + str(len(data)) + "条数据")
        data.insert(4, 'create_date', str(time.strftime("%Y-%m-%d", time.localtime())))
        data.to_sql("makret_data_daily", self.engine, if_exists="append", index=False)

    """
    循环获取一段时间内的数据 循环调用daily接口,使用trade_date参数，可能有些股票现在不存在
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
        # print type(data_1)
        # print data_1
        for index, row in data_1.iterrows():
            date_2 = row["cal_date"]
            date_2.decode('utf8')
            self.daily(trade_date=date_2)

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
        self.logger.info('TuShare weekly 周线行情 ts_code: ' + strUtils.noneToWdy(ts_code) + " trade_date： " +
                         strUtils.noneToWdy(trade_date) + " start_date： " +
                         strUtils.noneToWdy(start_date) + " end_date： " +
                         strUtils.noneToWdy(end_date) + ' 数据共：' + str(len(data)) + "条数据")
        data.insert(11, 'create_date', str(time.strftime("%Y-%m-%d", time.localtime())))
        data.to_sql("makret_data_weekly", self.engine, if_exists="append", index=False)

    """
        循环获取一段时间内的数据 循环调用weekly接口,使用trade_date参数，可能有些股票现在不存在
    """

    def weekly_cycle(self, exchange=None, start_date=None, end_date=None):
        # TODO 需要替换为获取每周第一个日期和每月第一个日期

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
            self.weekly(trade_date=date_2)
            time.sleep(0.4)

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
        self.logger.info('TuShare monthly 月线 ts_code: ' + strUtils.noneToWdy(ts_code) + " trade_date： " +
                         strUtils.noneToWdy(trade_date) + " start_date： " +
                         strUtils.noneToWdy(start_date) + " end_date： " +
                         strUtils.noneToWdy(end_date) + ' 数据共：' + str(len(data)) + "条数据")
        data.insert(11, 'create_date', str(time.strftime("%Y-%m-%d", time.localtime())))
        data.to_sql("makret_data_monthly", self.engine, if_exists="append", index=False)

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

    def quotes_daily(self, ts_code=None, trade_date=None, start_date=None, end_date=None):
        try:
            data = self.pro.daily(ts_code=ts_code, trade_date=trade_date, start_date=start_date, end_date=end_date)
            data.to_sql("quotes_daily", self.engine, if_exists="append", index=False)
            self.logger.info("TuShare 获取行情数据成功：ts_code：" + strUtils.noneToWdy(ts_code) + ", 交易日期：" +
                             strUtils.noneToWdy(trade_date) + ",  开始日期： " + strUtils.noneToWdy(start_date) +
                             ", 结束日期：  " + strUtils.noneToWdy(end_date) + " ,  数据量： " + str(len(data)) + "成功")
        except Exception as e:
            self.logger.errorlog(fun_name='daily', ts_code=ts_code, trade_date=trade_date, start_date=start_date,
                                 end_date=end_date, e=str(e))
            self.logger.error("TuShare 获取行情数据失败：code：" + strUtils.noneToWdy(ts_code) + ", 交易日期：" +
                              strUtils.noneToWdy(trade_date) + "  开始日期： " + strUtils.noneToWdy(start_date) +
                              ", 结束日期：  " + strUtils.noneToWdy(end_date) + " , 失败，错误详情： " + str(e))

    """
        沪深港通资金流向
        接口：moneyflow_hsgt
        描述：获取沪股通、深股通、港股通每日资金流向数据，每次最多返回300条记录，总量不限制。
        trade_date	str	N	交易日期 (二选一)
        start_date	str	N	开始日期 (二选一)
        end_date	str	N	结束日期
    """

    def moneyflow_hsgt(self, trade_date=None, start_date=None, end_date=None):
        data = self.pro.moneyflow_hsgt(trade_date=trade_date, start_date=start_date, end_date=end_date)
        self.logger.info('TuShare moneyflow_hsgt 沪深港通资金流向 trade_date: ' +
                         strUtils.noneToWdy(trade_date) + " start_date： " +
                         strUtils.noneToWdy(start_date) + " end_date： " +
                         strUtils.noneToWdy(end_date) + ' 数据共：' + str(len(data)) + "条数据")
        data.insert(8, 'create_date', str(time.strftime("%Y-%m-%d", time.localtime())))
        data.to_sql("makret_moneyflow_hsgt", self.engine, if_exists="append", index=False)

    """
        沪深股通十大成交股
        接口：hsgt_top10
        描述：获取沪股通、深股通每日前十大成交详细数据
        ts_code	str	N	股票代码（二选一）
        trade_date	str	N	交易日期（二选一）
        start_date	str	N	开始日期
        end_date	str	N	结束日期
        market_type	str	N	市场类型（1：沪市 3：深市）
    """

    def hsgt_top10(self, ts_code=None, trade_date=None, start_date=None, end_date=None, market_type=None):
        data = self.pro.hsgt_top10(ts_code=None, trade_date=trade_date, start_date=start_date, end_date=end_date, market_type=None)
        self.logger.info('TuShare hsgt_top10 沪深股通十大成交股 ts_code: ' + strUtils.noneToWdy(ts_code) + ", 交易日期：" +
                         strUtils.noneToWdy(trade_date) + " start_date： " +
                         strUtils.noneToWdy(start_date) + " end_date： " +
                         strUtils.noneToWdy(end_date) + ' 数据共：' + str(len(data)) + "条数据")
        data.insert(8, 'create_date', str(time.strftime("%Y-%m-%d", time.localtime())))
        data.to_sql("makret_hsgt_top10", self.engine, if_exists="append", index=False)


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
        data = self.pro.hk_hold(code=None, ts_code=None, trade_date=trade_date, start_date=start_date, end_date=end_date, exchange=None)
        self.logger.info('TuShare hk_hold 沪深港股通持股明细 ts_code: ' + strUtils.noneToWdy(ts_code) + ", 交易日期：" +
                         strUtils.noneToWdy(trade_date) + " start_date： " +
                         strUtils.noneToWdy(start_date) + " end_date： " +
                         strUtils.noneToWdy(end_date) + ' 数据共：' + str(len(data)) + "条数据")
        data.insert(8, 'create_date', str(time.strftime("%Y-%m-%d", time.localtime())))
        data.to_sql("makret_hsgt_top10", self.engine, if_exists="append", index=False)


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
                "TuShare 获取复权因子成功" + "日期：" + strUtils.noneToWdy(trade_date) + ",数据量： " + str(len(data)) + "成功")
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
            self.logger.info("TuShare 获取停复牌信息成功，ts_code：" + strUtils.noneToWdy(ts_code) + "停盘日期：" +
                             strUtils.noneToWdy(suspend_date) + "复牌日期: " + strUtils.noneToWdy(resume_date) +
                             ", 数据量：" + str(len(data)))
        except Exception as e:
            self.logger.error(fun_name='suspend', ts_code=ts_code, start_date=suspend_date, end_date=resume_date,
                              e=str(e))
            self.logger.error(
                "TuShare 获取停复牌信息失败，ts_code：" + strUtils.noneToWdy(ts_code) + "停盘日期：" +
                strUtils.noneToWdy(suspend_date) + "复牌日期: " + strUtils.noneToWdy(resume_date) + ", 数据获取失败" + str(e))

    """
    接口：daily_basic
    更新时间：交易日每日15点～17点之间
    描述：获取全部股票每日重要的基本面指标，可用于选股分析、报表展示等。
    ts_code	str	Y	股票代码（二选一）
    trade_date	str	N	交易日期 （二选一）
    start_date	str	N	开始日期(YYYYMMDD)
    end_date	str	N	结束日期(YYYYMMDD)
    """

    # 根据参数获取每日指标
    def get_daily_basic(self, ts_code=None, trade_date=None, start_date=None, end_date=None):
        try:
            data = self.pro.daily_basic(ts_code=ts_code, trade_date=trade_date, start_date=start_date,
                                        end_date=end_date)
            data.to_sql("quotes_daily_basic", self.engine, if_exists="append", index=False)
            self.logger.info("TuShare 获取基本面指标成功：ts_code：" + strUtils.noneToWdy(ts_code) + ", 交易日期：" +
                             strUtils.noneToWdy(trade_date) + ",  开始日期： " + strUtils.noneToWdy(start_date) +
                             ", 结束日期：  " + strUtils.noneToWdy(end_date) + " ,  数据量： " + str(len(data)) + "成功")
        except Exception as e:
            self.logger.errorlog(fun_name='daily_basic', ts_code=ts_code, trade_date=trade_date, start_date=start_date,
                                 end_date=end_date, e=str(e))
            self.logger.error("TuShare 获取基本面指标失败：code：" + strUtils.noneToWdy(ts_code) + ", 交易日期：" +
                              strUtils.noneToWdy(trade_date) + "  开始日期： " + strUtils.noneToWdy(start_date) +
                              ", 结束日期：  " + strUtils.noneToWdy(end_date) + " , 失败，错误详情： " + str(e))

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
            self.logger.info("TuShare 获取基本面指标成功：ts_code：" + strUtils.noneToWdy(ts_code) + ", 交易日期：" +
                             strUtils.noneToWdy(trade_date) + ",  开始日期： " + strUtils.noneToWdy(start_date) +
                             ", 结束日期：  " + strUtils.noneToWdy(end_date) + " ,  数据量： " + str(len(data)) + "成功")
        except Exception as e:
            self.logger.errorlog(fun_name='daily_basic', ts_code=ts_code, trade_date=trade_date, start_date=start_date,
                                 end_date=end_date, e=str(e))
            self.logger.error("TuShare 获取基本面指标失败：code：" + strUtils.noneToWdy(ts_code) + ", 交易日期：" +
                              strUtils.noneToWdy(trade_date) + "  开始日期： " + strUtils.noneToWdy(start_date) +
                              ", 结束日期：  " + strUtils.noneToWdy(end_date) + " , 失败，错误详情： " + str(e))
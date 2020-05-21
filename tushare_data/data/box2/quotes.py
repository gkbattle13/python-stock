#!/usr/bin/env python
# -*- coding: utf-8 -*-

from tushare_data.utils import strUtils


class quotes():

    # 初始化: 数据连接：(engine), tushare api：(pro), 日志：（logger）
    def __init__(self, engine, pro, logger):
        self.engine = engine
        self.pro = pro
        self.logger = logger

    """
    接口：daily
    更新时间：交易日每天15点～16点之间
    描述：获取股票行情数据，或通过通用行情接口获取数据，包含了前后复权数据．
    ts_code	str	N	股票代码（二选一）
    trade_date	str	N	交易日期（二选一）
    start_date	str	N	开始日期(YYYYMMDD)
    end_date	str	N	结束日期(YYYYMMDD)
    """

    def quotes_daily(self, ts_code=None, trade_date=None, start_date=None, end_date=None):
        try:
            data = self.pro.daily(ts_code=ts_code, trade_date=trade_date, start_date=start_date, end_date=end_date)
            data.to_sql("quotes_daily", self.engine, if_exists="append", index=False)
            self.logger.info("TuShare 获取行情数据成功：ts_code：" + strUtils.noneToUndecided(ts_code) + ", 交易日期：" +
                             strUtils.noneToUndecided(trade_date) + ",  开始日期： " + strUtils.noneToUndecided(start_date) +
                             ", 结束日期：  " + strUtils.noneToUndecided(end_date) + " ,  数据量： " + str(len(data)) + "成功")
        except Exception as e:
            self.logger.errorlog(fun_name='daily', ts_code=ts_code, trade_date=trade_date, start_date=start_date,
                                 end_date=end_date,e=str(e))
            self.logger.error("TuShare 获取行情数据失败：code：" + strUtils.noneToUndecided(ts_code) + ", 交易日期：" +
                              strUtils.noneToUndecided(trade_date) + "  开始日期： " + strUtils.noneToUndecided(start_date) +
                              ", 结束日期：  " + strUtils.noneToUndecided(end_date) + " , 失败，错误详情： " + str(e))

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
            self.logger.info("TuShare 获取复权因子成功" + "日期：" + strUtils.noneToUndecided(trade_date) + ",数据量： " + str(len(data)) + "成功")
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
            self.logger.error(fun_name='suspend', ts_code=ts_code,  start_date=suspend_date, end_date=resume_date, e=str(e))
            self.logger.error(
                "TuShare 获取停复牌信息失败，ts_code：" + strUtils.noneToUndecided(ts_code) + "停盘日期：" +
                strUtils.noneToUndecided(suspend_date) + "复牌日期: " + strUtils.noneToUndecided(resume_date) + ", 数据获取失败" + str(e))


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
            data = self.pro.daily_basic(ts_code=ts_code,trade_date=trade_date, start_date=start_date, end_date=end_date)
            data.to_sql("quotes_daily_basic", self.engine, if_exists="append", index=False)
            self.logger.info("TuShare 获取基本面指标成功：ts_code：" + strUtils.noneToUndecided(ts_code) + ", 交易日期：" +
                             strUtils.noneToUndecided(trade_date) + ",  开始日期： " + strUtils.noneToUndecided(start_date) +
                ", 结束日期：  " + strUtils.noneToUndecided(end_date) + " ,  数据量： " + str(len(data)) + "成功")
        except Exception as e:
            self.logger.errorlog(fun_name='daily_basic', ts_code=ts_code, trade_date=trade_date, start_date=start_date,
                                 end_date=end_date, e=str(e))
            self.logger.error("TuShare 获取基本面指标失败：code：" + strUtils.noneToUndecided(ts_code) + ", 交易日期：" +
                              strUtils.noneToUndecided(trade_date) + "  开始日期： " + strUtils.noneToUndecided(start_date) +
                              ", 结束日期：  " + strUtils.noneToUndecided(end_date) + " , 失败，错误详情： " + str(e))


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
            data = self.pro.pro_bar(ts_code=ts_code,trade_date=trade_date, start_date=start_date, end_date=end_date)
            data.to_sql("quotes_daily_basic", self.engine, if_exists="append", index=False)
            self.logger.info("TuShare 获取基本面指标成功：ts_code：" + strUtils.noneToUndecided(ts_code) + ", 交易日期：" +
                             strUtils.noneToUndecided(trade_date) + ",  开始日期： " + strUtils.noneToUndecided(start_date) +
                ", 结束日期：  " + strUtils.noneToUndecided(end_date) + " ,  数据量： " + str(len(data)) + "成功")
        except Exception as e:
            self.logger.errorlog(fun_name='daily_basic', ts_code=ts_code, trade_date=trade_date, start_date=start_date,
                                 end_date=end_date, e=str(e))
            self.logger.error("TuShare 获取基本面指标失败：code：" + strUtils.noneToUndecided(ts_code) + ", 交易日期：" +
                              strUtils.noneToUndecided(trade_date) + "  开始日期： " + strUtils.noneToUndecided(start_date) +
                              ", 结束日期：  " + strUtils.noneToUndecided(end_date) + " , 失败，错误详情： " + str(e))

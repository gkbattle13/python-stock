#!/usr/bin/env python
# -*- coding: utf-8 -*-

from tushare_data.utils import strUtils


class fund():

    # 初始化: 数据连接：(engine), tushare api：(pro), 日志：（logger）
    def __init__(self, engine, pro, logger):
        self.engine = engine
        self.pro = pro
        self.logger = logger

    """
    接口：fund_basic
    描述：获取公募基金数据列表，包括场内和场外基金
    积分：用户需要至少200积分才可以调取，具体请参阅积分获取办法
    输入参数
    名称	类型	必选	描述
    market	str	N	交易市场: E场内 O场外（默认E）
    """
    def basic(self, market=None):
        try:
            data = self.pro.fund_basic(market=None)
            data.to_sql("fund_basic", self.engine, if_exists="append", index=False)
            self.logger.info("TuShare 获取公墓基金数据列表：market：" + strUtils.noneToWdy(market) + " ,  数据量： " + str(len(data)) + "成功")
        except Exception as e:
            self.logger.errorlog(fun_name='fund_basic', e=str(e))
            self.logger.error("TuShare 获取公墓基金数据列表：market：" + strUtils.noneToWdy(market) + " , 失败，错误详情： " + str(e))


    """
    接口：fund_company
    描述：获取公募基金管理人列表
    积分：用户需要至少200积分才可以调取，具体请参阅积分获取办法
    """
    def company(self):
        try:
            data = self.pro.fund_company()
            data.to_sql("fund_company", self.engine, if_exists="append", index=False)
            self.logger.info("TuShare 获取公募基金管理人列表：数据量： " + str(len(data)) + "成功")
        except Exception as e:
            self.logger.errorlog(fun_name='fund_company', e=str(e))
            self.logger.error("TuShare 获取公募基金管理人列表： 失败，错误详情： " + str(e))

    """
    接口：fund_nav
    描述：获取公募基金净值数据
    积分：用户需要至少400积分才可以调取，具体请参阅积分获取办法
    ts_code	    str	N	TS基金代码 （二选一）
    end_date	str	N	净值日期 （二选一）
    """
    def nav(self,ts_code = None, end_date = None):
        try:
            data = self.pro.fund_nav(ts_code = ts_code, end_date = end_date)
            data.to_sql("fund_nav", self.engine, if_exists="append", index=False)
            self.logger.info("TuShare 获取公募基金净值数据：ts_code：" + strUtils.noneToWdy(ts_code) + ", end_date: " + strUtils.noneToWdy(end_date) + " ,  数据量： " + str(len(data)) + "成功")
        except Exception as e:
            self.logger.errorlog(fun_name='fund_nav', ts_code=ts_code, end_date = end_date, e=str(e))
            self.logger.error("TuShare 获取公募基金净值数据：ts_code：" + strUtils.noneToWdy(ts_code) + ", end_date: " + strUtils.noneToWdy(end_date) + " , 失败，错误详情： " + str(e))

    """
    接口：fund_div
    描述：获取公募基金分红数据
    积分：用户需要至少400积分才可以调取，具体请参阅积分获取办法
    """
    def div(self, ann_date = None, ex_date = None, pay_date = None, ts_code = None):
        try:
            data = self.pro.fund_div(ann_date = ann_date, ex_date = ex_date,pay_date = pay_date, ts_code = ts_code)
            data.to_sql("fund_div", self.engine, if_exists="append", index=False)
            self.logger.info("TuShare 获取公募基金分红数据：ts_code：" + strUtils.noneToWdy(ts_code) + ", ann_date: " + strUtils.noneToWdy(ann_date) + ", ex_date: " + strUtils.noneToWdy(ex_date) + ", pay_date: " + strUtils.noneToWdy(pay_date) + " ,  数据量： " + str(len(data)) + "成功")
        except Exception as e:
            self.logger.errorlog(fun_name='fund_div', ts_code=ts_code, start_date =  ann_date,end_date = ex_date, cal_date = pay_date, e=str(e))
            self.logger.error("TuShare 获取公募基金分红数据：ts_code：" + strUtils.noneToWdy(ts_code) + ", ann_date: " + strUtils.noneToWdy(ann_date) + ", ex_date: " + strUtils.noneToWdy(ex_date) + ", pay_date: " + strUtils.noneToWdy(pay_date) + " , 失败，错误详情： " + str(e))


    """
    接口：fund_portfolio
    描述：获取公募基金持仓数据，季度更新
    积分：用户需要至少1000积分才可以调取，具体请参阅积分获取办法
    """
    def portfolio(self, ts_code = None):
        try:
            data = self.pro.fund_portfolio(ts_code = ts_code)
            data.to_sql("fund_portfolio", self.engine, if_exists="append", index=False)
            self.logger.info("TuShare 获取公募基金持仓数据，季度更新：ts_code：" + strUtils.noneToWdy(ts_code) + " ,  数据量： " + str(len(data)) + "成功")
        except Exception as e:
            self.logger.errorlog(fun_name='fund_portfolio', ts_code=ts_code, e=str(e))
            self.logger.error("TuShare 获取公募基金持仓数据，季度更新：ts_code：" + strUtils.noneToWdy(ts_code) + " , 失败，错误详情： " + str(e))


    """
    接口：fund_daily
    描述：获取场内基金日线行情，类似股票日行情
    更新：每日收盘后2小时内
    限量：单次最大800行记录，总量不限制
    积分：用户需要至少500积分才可以调取，具体请参阅积分获取办法
    ts_code	str	N	基金代码（二选一）
    trade_date	str	N	交易日期（二选一）
    start_date	str	N	开始日期
    end_date	str	N	结束日期
    """
    def daily(self, ts_code = None):
        try:
            data = self.pro.fund_portfolio(ts_code = ts_code)
            data.to_sql("fund_daily", self.engine, if_exists="append", index=False)
            self.logger.info("TuShare 获取公募基金持仓数据，季度更新：ts_code：" + strUtils.noneToWdy(ts_code) + " ,  数据量： " + str(len(data)) + "成功")
        except Exception as e:
            self.logger.errorlog(fun_name='fund_daily', ts_code=ts_code, e=str(e))
            self.logger.error("TuShare 获取公募基金持仓数据，季度更新：ts_code：" + strUtils.noneToWdy(ts_code) + " , 失败，错误详情： " + str(e))

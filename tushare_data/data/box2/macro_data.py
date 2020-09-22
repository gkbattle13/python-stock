#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd

from tushare_data.utils import strUtils


# 宏观数据
class macro_data():
    # 初始化: 数据连接：(engine), tushare api：(pro), 日志：（logger）
    def __init__(self, engine, pro, logger):
        self.engine = engine
        self.pro = pro
        self.logger = logger

    def all(self, start, end):
        self.borrow(start_date=start, end_date=end)
        self.price(start_m=start, end_m=end)
        self.currency(start_m=start, end_m=end)

    """ 
        民间借贷利率   每天获取
        date	    str	N	日期
        start_date	str	N	开始日期
        end_date	str	N	结束日期
    """

    def borrow(self, date=None, start_date=None, end_date=None):
        try:
            info = "TuShare 宏观数据 温州民间借贷利率 "
            data = self.pro.wz_index(date=date, start_date=start_date, end_date=end_date)
            self.logger.info(
                info + "date: " + strUtils.noneToUndecided(date) + ", start_date: " + strUtils.noneToUndecided(
                    start_date) + ", end_date: " + strUtils.noneToUndecided(end_date) + ' 数据共：' + str(
                    len(data)) + "条数据")
            data.to_sql("macro_borrow_wz", self.engine, if_exists="append", index=False)

            info = "TuShare 宏观数据 广州民间借贷利率 "
            data = self.pro.gz_index(date=date, start_date=start_date, end_date=end_date)
            self.logger.info(
                info + "date: " + strUtils.noneToUndecided(date) + ", start_date: " + strUtils.noneToUndecided(
                    start_date) + ", end_date: " + strUtils.noneToUndecided(end_date) + ' 数据共：' + str(
                    len(data)) + "条数据")
            data.to_sql("macro_borrow_gz", self.engine, if_exists="append", index=False)


        except Exception as e:
            self.logger.error("宏观数据民间借贷利率 ERROE：" + str(e))

    """
        GDP数据   季度获取
        q	    str	N	季度（2019Q1表示，2019年第一季度）
        start_q	str	N	开始季度
        end_q	str	N	结束季度
    """

    def cn_gdp(self, q=None, start_q=None, end_q=None):
        try:
            info = "TuShare 宏观数据 GDP数据 "
            data = self.pro.cn_gdp(q=q, start_q=start_q, end_q=end_q)
            self.logger.info(
                info + "q: " + strUtils.noneToUndecided(q) + ", start_q: " + strUtils.noneToUndecided(
                    start_q) + ", end_q: " + strUtils.noneToUndecided(end_q) + ' 数据共：' + str(len(data)) + "条数据")
            data.to_sql("macro_cn_gdp", self.engine, if_exists="append", index=False)
        except Exception as e:
            self.logger.error("宏观数据GDP数据 ERROE：" + str(e))

    """ 
        居民消费价格指数   每月获取
        m	    str	N	月份（YYYYMM，下同），支持多个月份同时输入，逗号分隔
        start_m	str	N	开始月份
        end_m	str	N	结束月份
    """

    def price(self, m=None, start_m=None, end_m=None):
        try:
            info = "TuShare 宏观数据 居民消费价格指数 "
            data = self.pro.cn_cpi(m=m, start_m=start_m, end_m=end_m)
            self.logger.info(
                info + "m: " + strUtils.noneToUndecided(m) + ", start_m: " + strUtils.noneToUndecided(
                    start_m) + ", end_m: " + strUtils.noneToUndecided(end_m) + ' 数据共：' + str(len(data)) + "条数据")
            data.to_sql("macro_price_cpi", self.engine, if_exists="append", index=False)

            info = "TuShare 宏观数据 工业生产者出厂价格指数 "
            data = self.pro.cn_ppi(m=m, start_m=start_m, end_m=end_m)
            self.logger.info(
                info + "m: " + strUtils.noneToUndecided(m) + ", start_m: " + strUtils.noneToUndecided(
                    start_m) + ", end_m: " + strUtils.noneToUndecided(end_m) + ' 数据共：' + str(len(data)) + "条数据")
            data.to_sql("macro_price_ppi", self.engine, if_exists="append", index=False)

        except Exception as e:
            self.logger.error("宏观数据民间借贷利率 ERROE：" + str(e))

    """ 
        货币供应量   每月获取
        m	    str	N	月度（202001表示，2020年1月）
        start_m	str	N	开始月度
        end_m	str	N	结束月度
        fields	str	N	指定输出字段（e.g. fields='month,m0,m1,m2'）
    """

    def currency(self, m=None, start_m=None, end_m=None):
        try:
            info = "TuShare 宏观数据 货币供应量 "
            data = self.pro.cn_m(m=m, start_m=start_m, end_m=end_m)
            self.logger.info(
                info + "m: " + strUtils.noneToUndecided(m) + ", start_m: " + strUtils.noneToUndecided(
                    start_m) + ", end_m: " + strUtils.noneToUndecided(end_m) + ' 数据共：' + str(len(data)) + "条数据")
            data.to_sql("macro_currency", self.engine, if_exists="append", index=False)

        except Exception as e:
            self.logger.error("宏观数据货币供应量 ERROE：" + str(e))

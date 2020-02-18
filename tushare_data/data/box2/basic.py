#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sqlalchemy
from datetime import time
import pandas as pd
import time
import tushare as ts
from sqlalchemy.exc import ResourceClosedError
from tushare_data.utils import strUtils


# 获取股票基础数据
class basic():

    # 初始化: 数据连接：(engine), tushare api：(pro), 日志：（logger）
    def __init__(self, engine, pro, logger):
        self.engine = engine
        self.pro = pro
        self.logger = logger
        logger.info("sqlalchemy版本为：" + sqlalchemy.__version__ + "。 ts 版本：" + ts.__version__)  # 查看版本

    """
    股票列表
    接口：stock_basic   该接口可以一次性获取所有数据，不提供历史数据
    描述：获取基础信息数据，包括股票代码、名称、上市日期、退市日期等
    is_hs	    str	N	是否沪深港通标的，N否 H沪股通 S深股通
    list_status	str	N	上市状态： L上市 D退市 P暂停上市
    exchange_id	str	N	交易所 SSE上交所 SZSE深交所 HKEX港交所
    """

    def stock_basic(self, is_hs, list_status, exchange):
        try:
            # 调用tushare获取数据
            data = self.pro.stock_basic(is_hs=is_hs, list_status=list_status, exchange=exchange,
                                        fields='ts_code,symbol,name,fullname,enname,exchange,curr_type,list_status,list_date,delist_date,is_hs')
            self.logger.info('TuShare 获取基础数据成功，一共' + str(len(data)) + "条数据")

            # 转换字段名称，添加时间字段信息
            data.rename(
                columns={'totalAssets': 'total_assets', 'liquidAssets': 'liquid_assets', 'fixedAssets': 'fixed_assets',
                         'reservedPerShare': 'reserved_per_share', 'timeToMarket': 'time_to_market'}, inplace=True)
            data.insert(10, 'create_time', str(time.strftime("%Y-%m-%d", time.localtime())))

            # TODO 获取新数据与已有数据求差集，记录每天新增的数据
            # 获取本地已有数据，并转移到basic_stock_history表中
            sql = 'select * from basic_stock'
            df_mysql = pd.read_sql(sql, con=self.engine)
            self.logger.info('本地数据库基础数据一共' + str(len(df_mysql)) + "条数据")
            df_mysql.index.name = None
            df_mysql.to_sql("basic_stock_history", self.engine, if_exists="append", index=False)

            # 清空basic_stock表，存放当天数据, 在没有数据返回的时候会报错，
            # try:
            #     pd.read_sql_query("TRUNCATE basic_stock", con=self.engine)
            # except ResourceClosedError as e:
            #     self.logger.info(e)
            data.to_sql("basic_stock", self.engine, if_exists="append", index=False)
            #  Todo 准备求出差集，提升history中数据有效性
        except Exception as e:
            self.logger.error("TuShare 获取基本数据报错：" + str(e))

    """
    交易日期
    获取各大交易所交易日历数据, 默认提取的是上交所 2018年10月11号数据限制为1W条， 需要分开获取
    exchange_id	str	N	交易所 SSE上交所,SZSE深交所,CFFEX 中金所,SHFE 上期所,CZCE 郑商所,DCE 大商所,INE 上能源,IB 银行间,XHKG 港交所
    start_date	str	N	开始日期   
    end_date	str	N	结束日期  
    is_open	    int	N	是否交易 0休市 1交易
    """

    def trade_Cal(self, exchange=None, start_date=None, end_date=None):
        try:
            data = self.pro.trade_cal(exchange=exchange, start_date=start_date, end_date=end_date,
                                      fields="exchange,cal_date,is_open,pretrade_date")
            self.logger.info(
                'TuShare 获取 start_date: ' + strUtils.noneToWdy(start_date) + " 到end_date： " + strUtils.noneToWdy(
                    end_date) + ' 数据共：' + str(len(data)) + "条数据")
            data.to_sql("basic_trade_cal", self.engine, if_exists="append", index=False)
        except Exception as e:
            self.logger.error("TuShare 获取各大交易所交易日历数据：" + str(e))


    """
        接口：hs_const  
        描述：获取沪股通、深股通成分数据
        hs_type	str	Y	类型SH沪股通SZ深股通
        is_new	str	N	是否最新 1 是 0 否 (默认1)
    """

    def hs_const(self, hs_type, is_new=None):
        try:
            data = self.pro.hs_const(hs_type=hs_type, is_new=is_new)
            self.logger.info('TuShare 沪深股通成份股 hs_type: ' + strUtils.noneToWdy(hs_type) + " is_new： " + strUtils.noneToWdy(is_new) + ' 数据共：' + str(len(data)) + "条数据")
            data.insert(2, 'create_date', str(time.strftime("%Y-%m-%d", time.localtime())))
            data.to_sql("basic_hs_const", self.engine, if_exists="append", index=False)
        except Exception as e:
            self.logger.error("TuShare 获取沪股通、深股通成分数据：" + str(e))


    """
        接口：stk_managers  
        描述：上市公司管理层
        ts_code	str	Y	股票代码，支持单个或多个股票输入
    """

    def stk_managers(self, ts_code):
        try:
            data = self.pro.stk_managers(ts_code=ts_code)
            self.logger.info('TuShare 上市公司管理层 stk_managers: ' + strUtils.noneToWdy(ts_code) + ' 数据共：' + str(len(data)) + "条数据")
            data.insert(2, 'create_date', str(time.strftime("%Y-%m-%d", time.localtime())))
            data.to_sql("basic_stk_managers", self.engine, if_exists="append", index=False)
        except Exception as e:
            self.logger.error("TuShare 上市公司管理层：" + str(e))

    """
        接口：stk_rewards  
        描述：管理层薪酬和持股
        ts_code	  str	Y	TS股票代码，支持单个或多个代码输入
        end_date  str	N	报告期
    """

    def stk_rewards(self, ts_code):
        try:
            data = self.pro.stk_rewards(ts_code=ts_code, end_date=None)
            self.logger.info('TuShare 管理层薪酬和持股 stk_managers: ' + strUtils.noneToWdy(ts_code) + ' 数据共：' + str(len(data)) + "条数据")
            data.insert(2, 'create_date', str(time.strftime("%Y-%m-%d", time.localtime())))
            data.to_sql("basic_stk_rewards", self.engine, if_exists="append", index=False)
        except Exception as e:
            self.logger.error("TuShare 管理层薪酬和持股：" + str(e))


    """     
      接口：stock_company   会删除之前的信息，保留当天的
      描述：获取上市公司基础信息
    """

    def stock_company(self):
        try:
            data = self.pro.stock_company()
            self.logger.info('TuShare 上市公司基础信息：' + ' 数据共：' + str(len(data)) + "条数据")

            # 清空basic_stock_company表，存放当天数据, 在没有数据返回的时候会报错，
            # try:
                # pd.read_sql_query("TRUNCATE basic_stock_company", con=self.engine)
            # except ResourceClosedError as e:
            #     self.logger.info(e)

            data.insert(4, 'create_date', str(time.strftime("%Y-%m-%d", time.localtime())))
            data.to_sql("basic_stock_company", self.engine, if_exists="append", index=False)
        except Exception as e:
            self.logger.error("TuShare 股票曾用名：" + str(e))



    """
      股票曾用名    先删除在获取 
      接口：namechange
      描述：历史名称变更记录
      限量：单次最大10000
      ts_code	    str	N	TS代码
      start_date	str	N	公告开始日期
      end_date	str	N	公告结束日期
      """

    def name_change(self, ts_code=None, start_date=None, end_date=None):
        try:
            data = self.pro.namechange(ts_code=ts_code, start_date=start_date, end_date=end_date,
                                       fields='ts_code,name,start_date,end_date,ann_date,change_reason')
            self.logger.info('TuShare 股票曾用名 start_date: ' + strUtils.noneToWdy(start_date) + " 到end_date： " +
                             strUtils.noneToWdy(end_date) + ' 数据共：' + str(len(data)) + "条数据")
            # 清空basic_stock表，存放当天数据, 在没有数据返回的时候会报错，
            # try:
                # pd.read_sql_query("TRUNCATE basic_name_change", con=self.engine)
            # except ResourceClosedError as e:
            #     self.logger.info(e)

            data.insert(4, 'create_date', str(time.strftime("%Y-%m-%d", time.localtime())))
            data.to_sql("basic_name_change", self.engine, if_exists="append", index=False)
        except Exception as e:
            self.logger.error("TuShare 股票曾用名：" + str(e))

    """
      IPO新股列表   先删除在获取 
      接口：new_share
      描述：获取新股上市列表数据
      限量：单次最大2000条，总量不限制
      start_date	str	N	上网发行开始日期
      end_date	    str	N	上网发行结束日期
    """

    def new_share(self, start_date=None, end_date=None):
        try:
            data = self.pro.new_share(start_date=start_date, end_date=end_date)
            self.logger.info('TuShare IPO新股列表 start_date: ' + strUtils.noneToWdy(start_date) + " 到end_date： " +
                             strUtils.noneToWdy(end_date) + ' 数据共：' + str(len(data)) + "条数据")
            data.to_sql("basic_new_share", self.engine, if_exists="append", index=False)
        except Exception as e:
            self.logger.error("TuShare IPO新股列表：" + str(e))

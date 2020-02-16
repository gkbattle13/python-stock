#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
基础数据获取 包含列表，交易日历，曾用名，沪股通、深股通成分数据
Created on 2018/10/09
@author: Guokun
'''
import tushare as ts
import pandas as pd
from pandas import DataFrame
import sqlalchemy
import threading
import queue
import time
from tushare_data import utils as strUtils

from sqlalchemy.exc import ResourceClosedError

from tushare_data.utils.date import date_tool


class QuotesDataAll:

    # 初始化: 数据连接 engine, tushare api pro, logger
    def __init__(self, engine, pro, logger):
        self.engine = engine
        self.pro = pro
        self.logger = logger
        logger.info("sqlalchemy版本为：" + sqlalchemy.__version__ + "。 ts 版本：" + ts.__version__)  # 查看版本

    """
    1. 获取基本面数据 包括股票代码、名称、上市日期、退市日期等
    is_hs	    str	N	是否沪深港通标的，N否 H沪股通 S深股通
    list_status	str	N	上市状态 L上市 D退市 P暂停上市
    exchange_id	str	N	交易所 SSE上交所 SZSE深交所 HKEX港交所
    
    每天获取数据，旧数据放在basic_stock_history表中，当天数据放在basic_stock中
    """

    def stock_basic(self, is_hs=None, list_status=None, exchange_id=None):

        # 使用Tushare获取基本数据
        try:
            # 获取当天数据
            data = self.pro.stock_basic(is_hs=is_hs, list_status=list_status, exchange_id=exchange_id,
                                        fields='ts_code,symbol,name,fullname,enname,exchange_id,curr_type,list_status,list_date,delist_date,is_hs')
            self.logger.info('TuShare 获取基础数据成功，一共' + str(len(data)) + "条数据")

            """
            设置index 转换TuShare columns 和本地数据库一致
            DataFrame可以通过set_index方法，可以设置单索引和复合索引。
            DataFrame.set_index(keys, drop=True, append=False, inplace=False, verify_integrity=False)
            append添加新索引，drop为False，inplace为True时，索引将会还原为列
            """
            # data.set_index("ts_code", inplace=True, drop=True)
            self.logger.info(str(time.strftime("%Y-%m-%d", time.localtime())))
            data.rename(
                columns={'totalAssets': 'total_assets', 'liquidAssets': 'liquid_assets', 'fixedAssets': 'fixed_assets',
                         'reservedPerShare': 'reserved_per_share', 'timeToMarket': 'time_to_market'}, inplace=True)
            data.insert(10, 'create_time', str(time.strftime("%Y-%m-%d", time.localtime())))

            # 获取本地已有数据，并转移到basic_stock_history表中
            sql = 'select * from basic_stock'
            df_mysql = pd.read_sql(sql, con=self.engine)
            self.logger.info('本地数据库基础数据一共' + str(len(df_mysql)) + "条数据")
            df_mysql.index.name = None
            df_mysql.to_sql("basic_stock_history", self.engine, if_exists="append", index=False)

            # 清空basic_stock表，存放当天数据
            try:
                pd.read_sql_query("TRUNCATE basic_stock", con=self.engine)
            except ResourceClosedError as e:
                self.logger.info(e)
            data.to_sql("basic_stock", self.engine, if_exists="append", index=False)
            #  Todo 准备求出差集，提升history中数据有效性
        except Exception as e:
            self.logger.error("TuShare 获取基本数据报错：" + str(e))

    """
    获取各大交易所交易日历数据, 默认提取的是上交所 2018年10月11号数据限制为1W条
    exchange_id	str	N	交易所 SSE上交所 SZSE深交所  
    start_date	str	N	开始日期   
    end_date	str	N	结束日期  
    is_open	    int	N	是否交易 0休市 1交易
    """

    def trade_Cal(self, exchange_id=None, start_date=None, end_date=None):
        try:
            data = self.pro.trade_cal(exchange_id=exchange_id, start_date=start_date, end_date=end_date)
            self.logger.info('TuShare 获取交易日历数据: ' + strUtils.noneToWdy(start_date) + " 到end_date： " +
                             strUtils.noneToWdy(end_date) + ' 数据共：' + str(len(data)) + "条数据")
            data.to_sql("basic_trade_cal", self.engine, if_exists="append", index=False)
        except Exception as e:
            self.logger.error("TuShare 获取各大交易所交易日历数据：" + str(e))

    """
    股票曾用名
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
            self.logger.info(
                'TuShare 股票曾用名 start_date: ' + strUtils.noneToWdy(start_date) + " 到end_date： " + strUtils.noneToWdy(
                    end_date) + ' 数据共：' + str(len(data)) + "条数据")
            data.insert(4, 'create_date', str(time.strftime("%Y-%m-%d", time.localtime())))
            data.to_sql("basic_name_change", self.engine, if_exists="append", index=False)
        except Exception as e:
            self.logger.error("TuShare 股票曾用名：" + str(e))

    """
    接口：hs_const
    描述：获取沪股通、深股通成分数据
    限量：单次最大10000
    hs_type	str	Y	类型SH沪股通SZ深股通
    is_new	str	N	是否最新 1 是 0 否 (默认1)
    """

    def hs_const(self, hs_type, is_new=None):
        try:
            data = self.pro.hs_const(hs_type=hs_type, is_new=is_new)
            self.logger.info(
                'TuShare 沪深股通成份股 hs_type: ' + strUtils.noneToWdy(hs_type) + " is_new： " + strUtils.noneToWdy(
                    is_new) + ' 数据共：' + str(len(data)) + "条数据")
            data.insert(4, 'create_date', str(time.strftime("%Y-%m-%d", time.localtime())))
            data.to_sql("basic_hs_const", self.engine, if_exists="append", index=False)
        except Exception as e:
            self.logger.error("TuShare 沪深股通成份股：" + str(e))

    """ 
    接口：daily
    描述：获取股票行情数据, 根据basic表轮询出所有股票的历史数据，多线程获取历史数, 获取所有股票到目前为止的所有数据
    ts_code	    str	N	股票代码（二选一）
    trade_date	str	N	交易日期（二选一）
    start_date	str	N	开始日期
    end_date	str	N	结束日期
    """

    # 单线程获取, 在出错的时候添加参数进入error表
    def quotes_daily(self, ts_code=None, trade_date=None, start_date=None, end_date=None):
        try:
            data = self.pro.daily(ts_code=ts_code, trade_date=trade_date, start_date=start_date, end_date=end_date)
            data.to_sql("quotes_daily", self.engine, if_exists="append", index=False)
            self.logger.info(
                "curentThread: " + str(threading.current_thread()) + "日线行情数据，代码：" + strUtils.noneToWdy(ts_code) +
                "  开始日期： " + strUtils.noneToWdy(start_date) + ", 结束日期：  " + strUtils.noneToWdy(end_date) +
                " ,交易日期：" + strUtils.noneToWdy(trade_date) + ", 数据量： " + str(len(data)) + "成功")
        except Exception as e:
            d2 = DataFrame(
                {'fun_name': 'daily', 'ts_code': ts_code, 'trade_date': trade_date, 'start_date': start_date,
                 'end_date': start_date, 'time': [1],
                 'error_info': str(e), 'create_time': str(time.strftime("%Y-%m-%d", time.localtime()))})
            d2.to_sql("error_daily", self.engine, if_exists="append", index=False)
            self.logger.error(
                "Tushare 获取日线行情数据，代码：" + strUtils.noneToWdy(str(ts_code)) + "  开始日期： " + strUtils.noneToWdy(
                    start_date) +
                ", 结束日期: " + strUtils.noneToWdy(end_date) + " ,交易日期：" + strUtils.noneToWdy(
                    trade_date) + " 失败，错误详情： " + str(
                    e))

    # 通过trade_date 多线程获取数据
    queue_daily_trade = queue.Queue()

    def quotes_daily_trade(self, maxThread, begin_date, end_date=None):
        day_list = date_tool.getBetweenDayList(begin_date, end_date, format="%Y%m%d")
        # 开始多线程, 设置线程数
        for i in range(maxThread):
            t = threading.Thread(target=self.quotes_daily_thread)
            t.daemon = True
            t.start()
        # 遍历日期集合添加到queue中
        for qu in day_list:
            self.queue_daily_trade.put(qu)
        self.queue_daily_trade.join()

    def quotes_daily_thread(self):
        while True:
            i = self.queue_daily_trade.get()
            self.quotes_daily(trade_date=i)

    """
    接口：adj_factor
    描述：获取股票复权因子，可提取单只股票全部历史复权因子，也可以提取单日全部股票的复权因子。
    ts_code	    str	Y	股票代码
    trade_date	str	N	交易日期
    start_date	str	N	开始日期
    end_date	str	N	结束日期
    """

    # 单线程获取复权因子,在出错的时候存入数据进入error
    def adj_trade(self, ts_code=None, trade_date=None, start_date=None, end_date=None):
        try:
            data = self.pro.adj_factor(ts_code=ts_code, trade_date=trade_date, start_date=start_date, end_date=end_date)
            data.to_sql("quotes_adj_factor", self.engine, if_exists="append", index=False)
            self.logger.info("curentThread: " + str(threading.current_thread()) + "复权因子，日期：" + trade_date +
                             ",数据量： " + str(len(data)) + "成功")
        except Exception as e:
            self.error(fun_name='adj_trade', trade_date=trade_date, start_date=start_date, end_date=end_date, e=str(e))
            self.logger.error("curentThread: " + str(
                threading.current_thread()) + "复权因子，日期：" + trade_date + "获取数据失败，保存入error  " + str(e))

    queue_adj_factor = queue.Queue()

    # 通过trade_date 多线程获取数据
    def quotes_job_adj_trade(self, maxThread, begin_date, end_date=None):
        day_list = date_tool.getBetweenDayList(begin_date, end_date, format="%Y%m%d")
        # 开始多线程, 设置线程数
        for i in range(maxThread):
            t = threading.Thread(target=self.quotes_job_adj_thread)
            t.daemon = True
            t.start()
        # 遍历日期集合添加到queue中
        for qu in day_list:
            self.queue_adj_factor.put(qu)
        self.queue_adj_factor.join()

    def quotes_job_adj_thread(self):
        while True:
            i = self.queue_adj_factor.get()
            self.adj_trade(trade_date=i)

    """
    接口：suspend
    描述：获取股票每日停复牌信息
    ts_code	        str	N	股票代码(三选一)
    suspend_date	str	N	停牌日期(三选一)
    resume_date	    str	N	复牌日期(三选一)
    """
    queue_suspend = queue.Queue()

    # 单线程获取股票每日停复牌信息,在出错的时候存入数据进入error
    def suspend(self, ts_code=None, suspend_date=None, resume_date=None):
        try:
            data = self.pro.suspend(ts_code=ts_code, suspend_date=suspend_date, resume_date=resume_date)
            data.to_sql("quotes_suspend_copy1", self.engine, if_exists="append", index=False)
            self.logger.info(
                "curentThread: " + str(threading.current_thread()) + "停复牌信息，代码：" + strUtils.noneToWdy(ts_code)
                + "停盘日期：" + strUtils.noneToWdy(suspend_date) + "复牌日期: " + strUtils.noneToWdy(
                    resume_date) + ", 数据量：" + str(len(data)) + "成功")
        except Exception as e:
            self.error(fun_name='suspend', start_date=suspend_date, end_date=resume_date, e=str(e))
            self.logger.error(
                "curentThread: " + str(threading.current_thread()) + "停复牌信息，代码：" + strUtils.noneToWdy(ts_code)
                + "停盘日期：" + strUtils.noneToWdy(suspend_date) + "复牌日期: " + strUtils.noneToWdy(
                    resume_date) + ", 数据获取失败" + str(e))

    # 通过trade_date 多线程获取数据
    def quotes_suspend(self, maxThread, begin_date, end_date=None):
        day_list = date_tool.getBetweenDayList(begin_date, end_date, format="%Y%m%d")

        # 开始多线程, 设置线程数
        for i in range(maxThread):
            t = threading.Thread(target=self.quotes_suspend_thread)
            t.daemon = True
            t.start()
        # 遍历日期集合添加到queue中
        for qu in day_list:
            self.queue_suspend.put(qu)
        self.queue_suspend.join()

    def quotes_suspend_thread(self):
        while True:
            i = self.queue_suspend.get()
            # self.suspend(suspend_date=i)
            self.suspend(resume_date=i)

    # 每日指标
    queue_daily_basic = queue.Queue()

    def daily_basic(self):
        try:
            sql = 'select * from ts_stock_basic'
            df_mysql = pd.read_sql(sql, con=self.engine)
            self.logger.info('TuShare 本地数据库基础数据一共' + str(len(df_mysql)) + "条数据")
            print('本地数据库基础数据一共' + str(len(df_mysql)) + "条数据")
            for i in range(30):
                t = threading.Thread(target=self.job_daily_basic)
                t.daemon = True
                t.start()
            for index, qu in df_mysql.iterrows():
                self.queue_daily_basic.put(qu)
            self.queue_daily_basic.join()
            # self.logger.info('TuShare 获取 start_date: ' + start_date +" 到end_date： " + end_date +  ' 数据共：' + str(len(data)) + "条数据")
            # print('TuShare 获取 start_date: ' + start_date +" 到end_date： " + end_date +  ' 数据共：' + str(len(data)) + "条数据")
        except Exception as e:
            self.logger.error("TuShare 获取基础数据报错：" + str(e))
            print("TuShare 获取基础数据报错：" + str(e))

    # 多线程job，Tushare 获取数据，只能获取4000条数据，以1990到2001,2001到2010，2010 到 2018年为节点分开获取，获取失败的数据会存入 error_daily
    def job_daily_basic(self, ):
        while True:
            i = self.queue_daily_basic.get()
            list_date = int(i["list_date"])
            if (list_date < 20000101):
                self.get_daily_basic(i["ts_code"], start_date=i["list_date"], end_date='20000101')
                self.get_daily_basic(i["ts_code"], start_date="20000102", end_date='20100101')
                self.get_daily_basic(i["ts_code"], start_date="20100102", end_date='20180830')
            elif (list_date < 20100101):
                self.get_daily_basic(i["ts_code"], start_date=i["list_date"], end_date='20100101')
                self.get_daily_basic(i["ts_code"], start_date="20100102", end_date='20180830')
            else:
                self.get_daily_basic(i["ts_code"], start_date=i["list_date"], end_date='20180830')

    # 根据参数获取每日指标
    def get_daily_basic(self, ts_code, start_date, end_date):
        try:
            # thshare 接口数据限制，一次只能取4000条数据
            data = self.pro.daily_basic(ts_code=ts_code, start_date=start_date, end_date=end_date)
            data.set_index("ts_code", inplace=True, drop=True)
            data.to_sql("ts_daily_basic", self.engine, if_exists="append")
            self.logger.info(
                "curentThread: " + str(threading.current_thread()) + "每日指标数据，代码：" + str(ts_code) + ",  开始日期：" + str(
                    start_date) + ", 结束日期： " + str(end_date) + ", 数据量：" + str(len(data)))
            print("curentThread: " + str(threading.current_thread()) + "每日指标数据，代码：" + str(ts_code) + ",  开始日期：" + str(
                start_date) + ", 结束日期： " + str(end_date) + ", 数据量：" + str(len(data)))
        except Exception as e:
            d2 = DataFrame({
                'fun_name': [str("daily_basic")],
                'ts_code': [str(ts_code)],
                'start_date': [str(start_date)],
                'error_info': [str(e)],
                'end_date': [str(end_date)],
                'create_time': [str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))],
                'time': [1]
            })
            d2.set_index("ts_code", inplace=True, drop=True)
            d2.to_sql("error_daily", self.engine, if_exists="append")

    # 根据error表获取报错数据，并再出获取
    def get_error_daily_basic(self, ):
        sql = "select * from error_daily where fun_name ='daily_basic'"
        data = pd.read_sql(sql, con=self.engine)
        for index, i in data.iterrows():
            try:
                data = self.pro.daily_basic(ts_code=i["ts_code"], start_date=i["start_date"], end_date=i["end_date"])
                data.set_index("ts_code", inplace=True, drop=True)
                data.to_sql("ts_daily_basic", self.engine, if_exists="append")
                self.logger.error("代码：" + str(i["ts_code"]) + " 开始日期： " + i["start_date"] + " 结束日期： " + i[
                    "end_date"] + ",  " + "数据量： " + str(len(data)) + "成功")
                print("代码：" + str(i["ts_code"]) + " 开始日期： " + i["start_date"] + " 结束日期： " + i[
                    "end_date"] + ",  " + "数据量： " + str(len(data)) + "成功")
                sqlDel = 'delete from error_daily where ts_code = "' + str(
                    i["ts_code"]) + '"  and  fun_name = "daily_basic"'
                pd.read_sql_query(sqlDel, con=self.engine)
                print("代码：" + str(i["ts_code"]) + "获取数据成功，删除数据库成功")
            except Exception as e:
                self.logger.error("Tushare 获取日线行情数据，代码：" + str(i["ts_code"]) + "失败，错误详情： " + str(e))
                print("Tushare 获取日线行情数据，代码：" + str(i["ts_code"]) + "失败，错误详情： " + str(e))

    # 调用daily_error, 轮询error表，重新获取数据，如果成功则删除该条消息
    def daily_error(self, ):
        sql = 'select ts_code, start_date, end_date from error_daily'
        df_mysql = pd.read_sql(sql, con=self.engine)
        self.logger.info('本地数据库基础报错数据一共' + str(len(df_mysql)) + "条数据")
        print('本地数据库基础报错数据一共' + str(len(df_mysql)) + "条数据")

        for index, i in df_mysql.iterrows():
            try:
                data = self.pro.daily(ts_code=i["ts_code"], start_date=i["start_date"], end_date=i["end_date"])
                data.set_index("ts_code", inplace=True, drop=True)
                data.to_sql("ts_daily", self.engine, if_exists="append")
                self.logger.error("代码：" + str(i["ts_code"]) + " 开始日期： " + i["start_date"] + " 结束日期： " + i[
                    "end_date"] + ",  " + "数据量： " + str(len(data)) + "成功")
                print("代码：" + str(i["ts_code"]) + " 开始日期： " + i["start_date"] + " 结束日期： " + i[
                    "end_date"] + ",  " + "数据量： " + str(len(data)) + "成功")
                sqlDel = 'delete from error_daily where ts_code = "' + str(i["ts_code"]) + '"'
                pd.read_sql_query(sqlDel, con=self.engine)
                print("代码：" + str(i["ts_code"]) + "获取数据成功，删除数据库成功")
            except Exception as e:
                self.logger.error("Tushare 获取日线行情数据，代码：" + str(i["ts_code"]) + "失败，错误详情： " + str(e))



    # 获取基础数据
    # stock_basic()
    # 获取19000101到现在的数据
    # tradeCal('SSE', '19000101', '20180829')
    # 获取19000101到20180830的数据，
    # daily()
    # 重新获取数据
    # daily_error()
    # 获取复权因子
    # adj_factor()
    # 获取停复牌信息
    # suspend()
    # 每日指标
    # daily_basic()
    # 读取error中 daily_basic 数据
    # get_error_daily_basic()

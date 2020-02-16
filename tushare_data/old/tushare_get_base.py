# -*- coding: utf-8 -*-
"""
用于获取:
    沪深股票列表
    业绩预告
    业绩报告（主表）
    盈利能力数据
    营运能力数据
    成长能力数据
    偿债能力数据
    现金流量数据

Created on 2017-09-01
@author: Guo Kun
@contact: guokun@163.com
"""

"""
2018-08-24 ----- 获取数据为 2001 1 季度到 2018 2季度数据 ------- 2001 1季度  单季度成长 数据获取失败
"""

import pandas as pd
from sqlalchemy import create_engine
import tushare as ts
import time
import tushare_data.utils.date.date_tool as date_tool
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from termcolor import *
from tushare_data.utils import loggerUtils

# 防止报编码错误
# reload(sys)
# sys.setdefaultencoding('utf8')

logger = loggerUtils.TNLog()
# 创建Mysql数据库连接
# engine = create_engine('mysql://wahaha:ceshi@106.14.238.126:3306/stock?charset=utf8')
logger.info("sqlalchemy版本为：" + sqlalchemy.__version__)  # 查看版本
engine = create_engine('mysql://wahaha:ceshi@106.14.238.126:3306/stock1?charset=utf8')
Session = sessionmaker(bind=engine)
session = Session()

# 获取基本面数据 基本面数据并非都是一直不变，交易日都会有变化
def getBasic():
    # 使用Tushare获取基本数据
    try:
        df_basic = ts.get_stock_basics()
        logger.info('TuShare 获取基础数据成功，一共' + str(len(df_basic)) + "条数据")
        # 获取本地已有数据
        sql = 'select * from ts_basic'
        df_mysql = pd.read_sql(sql, con=engine)
        logger.info('本地数据库基础数据一共' + str(len(df_mysql)) + "条数据")
        # 设置index 转换TuShare columns 和本地数据库一致
        # DataFrame可以通过set_index方法，可以设置单索引和复合索引。
        # DataFrame.set_index(keys, drop=True, append=False, inplace=False, verify_integrity=False)
        # append添加新索引，drop为False，inplace为True时，索引将会还原为列
        df_mysql.set_index("code", inplace=True, drop=True)
        # 转换为数据库名称
        df_basic.rename(columns={'totalAssets':'total_assets', 'liquidAssets':'liquid_assets', 'fixedAssets':'fixed_assets',
                                 'reservedPerShare': 'reserved_per_share', 'timeToMarket': 'time_to_market'}, inplace = True)
        df_basic.insert(22,'update_time',time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        df_basic.to_sql("ts_basic", engine, if_exists="append")
        logger.debug(df_basic.columns)
    except Exception as e:
        logger.error("TuShare 获取基本数据报错：" + e)


# 获取单季度业绩报告
def getReportOne(year,quarter):
    try:
        # 开始获取单季度业绩报告
        logger.info("TuShare 开始获取" + str(year) + "年" + str(quarter) + "季度单季度业绩报告")
        df_report = ts.get_report_data(year, quarter)
        # 设置索引
        df_report.set_index('code', inplace=True, drop=True)
        df_report["year"] = year
        df_report["quarter"] = quarter
        df_report.to_sql("ts_basic_report", engine, if_exists="append")
        logger.info("TuShare 获取 " + str(year) + "年 " + str(quarter) + "季度单季度业绩报告保存成功")
    except Exception as e:
        logger.error("获取 " + str(year) + "年 " + str(quarter) + "季度单季度业绩报告数据报错  " + str(e))


# 获取单季度盈利能力
def getProfitOne(year,quarter):
    try:
        logger.info("TuShare 开始获取" + str(year) + "年" + str(quarter) + "季度单季度盈利能力")
        # 获取Tushare 数据开始
        df_profit = ts.get_profit_data(year, quarter)
        # 设置索引
        df_profit.set_index('code', inplace=True, drop=True)
        df_profit["year"] = year
        df_profit["quarter"] = quarter
        df_profit.to_sql("ts_basic_profit", engine, if_exists="append")
        logger.info("TuShare 获取" + str(year) + "年" + str(quarter) + "季度单季度盈利能力保存成功")
    except Exception as e:
        logger.error("TuShare 获取" + str(year) + "年" + str(quarter) + "季度单季度盈利能力数据异常 " + str(e))


# 获取单季度运营能力
def getOperationOne(year,quarter):
    try:
        logger.info("TuShare 开始获取" + str(year) + "年" + str(quarter) + "季度单季度运营能力")
        df_report = ts.get_operation_data(year, quarter) #获取运营能力
        df_report.set_index('code', inplace=True, drop=True)
        df_report.rename(columns={'arturnover': 'arturn_over', 'arturndays': 'arturn_days',
             'cashratio': 'cash_ratio','icratio': 'ic_ratio','sheqratio': 'sheq_ratio','adratio': 'ad_ratio'}, inplace=True)
        df_report["year"] = year
        df_report["quarter"] = quarter
        df_report.to_sql("ts_basic_operation", engine, if_exists="append");
        logger.info("TuShare 获取" + str(year) + "年" + str(quarter) + "季度单季度运营能力保存成功")
    except Exception as e:
        logger.error("TuShare 获取" + str(year) + "年" + str(quarter) + "季度运单季度运营能力数据异常 " + str(e))


# 获取单季度成长能力
def getGrowthOne(year,quarter):
    try:
        logger.info("TuShare 开始获取" + str(year) + "年" + str(quarter) + "季度 成长能力")
        df_growth = ts.get_growth_data(year, quarter)
        df_growth.set_index('code', inplace=True, drop=True)
        df_growth["year"] = year
        df_growth["quarter"] = quarter
        df_growth.to_sql("ts_basic_growth", engine, if_exists="append")
        logger.info("TuShare 获取" + str(year) + "年" + str(quarter) + "季度 成长能力数据数据库保存成功")
    except Exception as e:
        logger.error("TuShare 获取" + str(year) + "年" + str(quarter) + "季度 单季度成长能异常 " + str(e))

# 获取单季度偿债能力
def getDebtpayingOne(year,quarter):
    try:
        logger.info("TuShare 开始获取" + str(year) + "年" + str(quarter) + "季度 偿债能力")
        df_report = ts.get_debtpaying_data(year, quarter)
        df_report = df_report.replace('--', pd.np.NaN, regex=False)
        df_report.set_index('code', inplace=True, drop=True)
        df_report.rename(columns={'currentratio': 'current_ratio', 'quickratio': 'quick_ratio',
             'cashratio': 'cash_ratio','icratio': 'ic_ratio','sheqratio': 'sheq_ratio','adratio': 'ad_ratio'}, inplace=True)
        df_report["year"] = year
        df_report["quarter"] = quarter
        df_report.to_sql("ts_basic_debt_paying", engine, if_exists="append")
        logger.info("TuShare 获取" + str(year) + "年" + str(quarter) + "季度 偿债能力保存成功")
    except Exception as e:
        logger.error("TuShare 获取" + str(year) + "年" + str(quarter) + "季度 偿债能力异常 " + str(e))

# 获取单季度现金流量
def getCashflowOne(year,quarter):
    try:
        logger.info("TuShare 开始获取" + str(year) + "年" + str(quarter) + "季度 现金流量")
        df_cashflow = ts.get_cashflow_data(year, quarter)
        df_cashflow.set_index('code', inplace=True, drop=True)
        df_cashflow.rename(columns={'rateofreturn': 'rate_of_return', 'cashflowratio': 'cash_flow_ratio'}, inplace=True)
        df_cashflow["year"] = year
        df_cashflow["quarter"] = quarter
        df_cashflow.to_sql("ts_basic_cashflow", engine, if_exists="append")
        logger.info("TuShare 获取" + str(year) + "年" + str(quarter) + "季度 现金流量保存成功")
    except Exception as e:
        logger.error("TuShare 获取" + str(year) + "年" + str(quarter) + "季度 现金流量异常 " + str(e))





# 根据list循环获取业绩报告
def getReportList(quarter_list):
    try:
        for qu in quarter_list:
            # 查询数据库中是否存在该年份该季度的数据
            sql = "select * from ts_basic_report where year = " + qu[0:4] + " and quarter = " + qu[-1];
            print(sql)
            hava_df = session.execute(sql)
            # 判断数据大小
            hava_df_list = list(hava_df)
            length = len(hava_df_list)
            if length < 1 :
                logger.info("数据库中不存在" + qu[0:4] + "年" + qu[-1] + "季度报表，开始调用接口获取")
                getReportOne(int(qu[0:4]), int(qu[-1]))
            else:
                logger.info("数据库中已存在" + qu[0:4] + "年" + qu[-1] + "季度报表,数据量为：" + str(length))
    except IOError as e:
        logger.error(colored("IOError: 获取业绩报告" + str(e), "blue", attrs=['bold']))
    except Exception as eb:
        logger.error(colored("获取业绩报告: " + eb.message, "red", attrs=['bold']))


# 根据list循环获取盈利能力
def getProfitList(quarter_list):
    try:
        for qu in quarter_list:
            # 查询数据库中是否存在该年份该季度的数据
            sql = "select * from ts_basic_profit where year = " + qu[0:4] + " and quarter = " + qu[-1];
            logger.info(sql)
            hava_df = session.execute(sql)
            # 判断数据大小
            hava_df_list = list(hava_df)
            length = len(hava_df_list)
            if length < 1:
                logger.info("数据库中不存在" + qu[0:4] + "年" + qu[-1] + "季度盈利能力，开始调用接口获取")
                getProfitOne(int(qu[0:4]), int(qu[-1]))
            else:
                logger.info("数据库中已存在" + qu[0:4] + "年" + qu[-1] + "季度盈利能力,数据量为：" + str(length))
    except IOError as e:
        logger.error(colored("IOError: 获取盈利能力" + str(e), "blue", attrs=['bold']))
    except Exception as eb:
        logger.error(colored("获取盈利能力: " + eb.message, "red", attrs=['bold']))


# 根据list循环获取运营能力
def getOperationList(quarter_list):
    try:
        for qu in quarter_list:
            # 查询数据库中是否存在该年份该季度的数据
            sql = "select * from ts_basic_operation where year = " + qu[0:4] + " and quarter = " + qu[-1];
            logger.info(sql)
            hava_df = session.execute(sql)
            # 判断数据大小
            hava_df_list = list(hava_df)
            length = len(hava_df_list)
            if length < 1:
                logger.info("数据库中不存在" + qu[0:4] + "年" + qu[-1] + "季度运营能力，开始调用接口获取")
                getOperationOne(int(qu[0:4]), int(qu[-1]))
            else:
                logger.info("数据库中已存在" + qu[0:4] + "年" + qu[-1] + "季度运营能力,数据量为：" + str(length))
    except IOError as e:
        logger.error(colored("IOError: 获取运营能力" + str(e), "blue", attrs=['bold']))
    except Exception as eb:
        logger.error(colored("获取运营能力: " + eb.message, "red", attrs=['bold']))


# 根据list循环获取成长能力
def getGrowthList(quarter_list):
    try:
        for qu in quarter_list:
            # 查询数据库中是否存在该年份该季度的数据
            sql = "select * from ts_basic_growth where year = " + qu[0:4] + " and quarter = " + qu[-1];
            logger.info(sql)
            hava_df = session.execute(sql)
            # 判断数据大小
            hava_df_list = list(hava_df)
            length = len(hava_df_list)
            if length < 1:
                logger.info("数据库中不存在" + qu[0:4] + "年" + qu[-1] + "季度成长能力，开始调用接口获取")
                getGrowthOne(int(qu[0:4]), int(qu[-1]))
            else:
                logger.info("数据库中已存在" + qu[0:4] + "年" + qu[-1] + "季度成长能力,数据量为：" + str(length))
    except IOError as e:
        logger.error(colored("IOError: 获取成长能力" + str(e), "blue", attrs=['bold']))
    except Exception as eb:
        logger.error(colored("获取成长能力: " + eb.message, "red", attrs=['bold']))


# 根据list循环获取偿债能力
def getDebtpayingList(quarter_list):
    try:
        for qu in quarter_list:
            # 查询数据库中是否存在该年份该季度的数据
            sql = "select * from ts_basic_debt_paying where year = " + qu[0:4] + " and quarter = " + qu[-1];
            logger.info(sql)
            hava_df = session.execute(sql)
            # 判断数据大小
            hava_df_list = list(hava_df)
            length = len(hava_df_list)
            if length < 1:
                logger.info("数据库中不存在" + qu[0:4] + "年" + qu[-1] + "季度偿债能力，开始调用接口获取")
                getDebtpayingOne(int(qu[0:4]), int(qu[-1]))
            else:
                logger.info("数据库中已存在" + qu[0:4] + "年" + qu[-1] + "季度偿债能力,数据量为：" + str(length))
    except IOError as e:
        logger.error(colored("IOError: 获取偿债能力" + str(e), "blue", attrs=['bold']))
    except Exception as eb:
        logger.error(colored("获取偿债能力: " + eb.message, "red", attrs=['bold']))


# 根据list循环获取现金流量
def getCashflowList(quarter_list):
    try:
        for qu in quarter_list:
            # 查询数据库中是否存在该年份该季度的数据
            sql = "select * from ts_basic_cashflow where year = " + qu[0:4] + " and quarter = " + qu[-1];
            logger.info(sql)
            hava_df = session.execute(sql)
            # 判断数据大小
            hava_df_list = list(hava_df)
            length = len(hava_df_list)
            if length < 1:
                logger.info("数据库中不存在" + qu[0:4] + "年" + qu[-1] + "季度现金流量，开始调用接口获取")
                getCashflowOne(int(qu[0:4]), int(qu[-1]))
            else:
                logger.info("数据库中已存在" + qu[0:4] + "年" + qu[-1] + "季度现金流量,数据量为：" + str(length))
    except IOError as e:
        logger.error(colored("IOError: 获取现金流量" + str(e), "blue", attrs=['bold']))
    except Exception as eb:
        logger.error(colored("获取现金流量: " + str(eb), "red", attrs=['bold']))


# 获取所有循环接口
def getReport1(beginDate):
    list = date_tool.getBetweenQuarter(beginDate)
    logger.info(list)
    # 循环获取业绩报告
    # getReportList(list)
    # # 循环获取盈利能力
    # getProfitList(list)
    # # 循环获取运营能力
    # getOperationList(list)
    # # 循环获取成长能力
    # getGrowthList(list)
    # # 循环获取偿债能力    `
    # getDebtpayingList(list)
    # # 循环获取现金流量
    # getCashflowList(list)


getGrowthOne(2001,1)

# getReport1("2001-01-01")

# sql = "select * from report where year = " + "2017" + " and quarter = " + "1";
# print sql
# hava_df = session.execute(sql)
# hava_df_list = list(hava_df)
# print len(hava_df_list)
# print type(hava_df)
# print hava_df


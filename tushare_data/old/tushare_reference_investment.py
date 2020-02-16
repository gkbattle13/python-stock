#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
投资参考数据:
    投资参考提供一些可能会影响股票价格走势的信息数据，为投资者在做投资决策时提供数据参考，
    亦可作为量化策略模型的事件因子纳入模型的计算。TuShare提供的参考数据主要包括以下：
        分配预案
        业绩预告
        限售股解禁
        基金持股
        新股上市
        融资融券（沪市）
        融资融券（深市）
    Created on 2017-09-04
    @author: Guo Kun
    @contact: guokun@163.com
"""

from sqlalchemy import create_engine
import tushare as ts
import tushare_data.utils.date.date_tool as date_tool
import sqlalchemy
from sqlalchemy.orm import sessionmaker

# 防止报编码错误
# reload(sys)
from tushare_data.utils import loggerUtils

logger = loggerUtils.TNLog()
logger.info("sqlalchemy版本为：" + sqlalchemy.__version__)  # 查看版本
engine = create_engine('mysql://wahaha:ceshi@106.14.238.126:3306/stock1?charset=utf8');
Session = sessionmaker(bind=engine)
session = Session()

# 分配预案
# year : 预案公布的年份，默认为2014
# top :取最新n条数据，默认取最近公布的25条
# retry_count：当网络异常后重试次数，默认为3
# pause:重试时停顿秒数，默认为0
def profitData(year,top):
    # 每到季报、年报公布的时段，就经常会有上市公司利润分配预案发布，
    # 而一些高送转高分红的股票往往会成为市场炒作的热点。及时获取和
    # 统计高送转预案的股票是参与热点炒作的关键，TuShare提供了简洁
    # 的接口，能返回股票的送转和分红预案情况。
    # 获取基本数据
    try:
        logger.info("Tushare 开始获取" + str(year) + "年分配预案数据：" + str(top) + "条")
        df_report = ts.profit_data(year, top)
        df_report.set_index('code', inplace=True, drop=True)
        df_report["year"] = year
        df_report.to_sql("ts_reference_profit", engine, if_exists="append")
        logger.info("Tushare 获取" + str(year) + "年分配预案数据：" + str(len(df_report)) + "条数据数据库保存成功")
    except Exception as e:
        logger.error("TuShare 获取" + str(year) + "年分配预案数据报错" + e)

# 获取单季度业绩预告
# year:int 年度 e.g:2014
# quarter:int 季度 :1、2、3、4，只能输入这4个季度
def forecastData(year,quarter):
    # 按年度、季度获取业绩预告数据，接口提供从1998年以后每年的业绩预告数据，需指定年度、季度两个参数。
    # 数据在获取的过程中，会打印进度信息(下同)。
    if year < 1998:
        year = 1998
    print("开始获取" + str(year) + "年" + str(quarter) + "业绩预告")
    df_forecast = ts.forecast_data(year, quarter)
    df_forecast.set_index('code', inplace=True, drop=True)
    df_forecast["year"] = year
    df_forecast["quarter"] = quarter
    print("获取" + str(year) + "年" + str(quarter) + "季度业绩预告成功")
    df_forecast.to_sql("ts_reference_forecast", engine, if_exists="append");
    print("获取" + str(year) + "年" + str(quarter) + "季度业绩预告数据库保存成功")


# 获取单月份限售股解禁
# year:年份,默认为当前年
# month:解禁月份，默认为当前月
# retry_count：当网络异常后重试次数，默认为3
# pause:重试时停顿秒数，默认为0
def xsgData(year,month):
    # 以月的形式返回限售股解禁情况，通过了解解禁股本的大小，判断股票上行的压力。
    # 可通过设定年份和月份参数获取不同时段的数据
    print("开始获取" + str(year) + "年" + str(month) + "限售股解禁")
    df_xsg = ts.xsg_data(year, month)
    df_xsg.set_index('code', inplace=True, drop=True)
    df_xsg["year"] = year
    df_xsg["month"] = month
    print("获取" + str(year) + "年" + str(month) + "月限售股解禁数据成功")
    df_xsg.to_sql("ts_reference_xsg", engine, if_exists="append");
    print("获取" + str(year) + "年" + str(month) + "月限售股解禁数据数据库保存成功")


# 获取单季度基金持股
def fundHoldings(year,quarter):
    print("开始获取" + str(year) + "年" + str(quarter) + "季度基金持股")
    df_xsg = ts.fund_holdings(year, quarter)
    df_xsg.set_index('code', inplace=True, drop=True)
    df_xsg["year"] = year
    df_xsg["quarter"] = quarter
    print("获取" + str(year) + "年" + str(quarter) + "季度基金持股数据成功")
    df_xsg.to_sql("ts_reference_fund_holdings", engine, if_exists="append");
    print("获取" + str(year) + "年" + str(quarter) + "季度基金持股数据库保存成功")


# 获取新股数据
def fundHoldings():
    print("开始获取新股数据")
    df_xsg = ts.new_stocks()
    df_xsg.set_index('code', inplace=True, drop=True)
    print("获取新股数据成功")
    df_xsg.to_sql("ts_reference_new_stocks", engine, if_exists="append");
    print("获取新股数据数据库保存成功")


# 根据list循环获取业绩报告
# def getReportList(quarter_list):
#     try:
#         for qu in quarter_list:
#             # 查询数据库中是否存在该年份该季度的数据
#             sql = "select * from report where year = " + qu[0:4] + " and quarter = " + qu[-1];
#             print(sql)
#             hava_df = session.execute(sql)
#             # 判断数据大小
#             hava_df_list = list(hava_df)
#             length = len(hava_df_list)
#             if length < 1 :
#                 print("数据库中不存在" + qu[0:4] + "年" + qu[-1] + "季度报表，开始调用接口获取")
#                 getReportOne(int(qu[0:4]), int(qu[-1]))
#             else:
#                 print("数据库中已存在" + qu[0:4] + "年" + qu[-1] + "季度报表,数据量为：" + str(length))
#     except IOError as e:
#         print(colored("IOError: 获取业绩报告" + e.message, "blue", attrs=['bold']))
#     except Exception as eb:
#         print(colored("获取业绩报告: " + eb.message, "red", attrs=['bold']))


# 循环获取分配预案
def getReport1(beginDate):
    quarter_list = date_tool.getBetweenQuarter(beginDate)
    print(list)
    # try:
    for qu in quarter_list:
            # 查询数据库中是否存在该年份该季度的数据
            # sql = "select * from ts_basic_cashflow where year = " + qu[0:4] + " and quarter = " + qu[-1];
            # logger.info(sql)
            # hava_df = session.execute(sql)
            # 判断数据大小
            # hava_df_list = list(hava_df)
            # length = len(hava_df_list)
            # if length < 1:
            #     logger.info("数据库中不存在" + qu[0:4] + "年" + qu[-1] + "季度现金流量，开始调用接口获取")
        profitData(int(qu[0:4]), 1000)
            # else:
            #     logger.info("数据库中已存在" + qu[0:4] + "年" + qu[-1] + "季度现金流量,数据量为：" + str(length))
    # except IOError as e:
    #     logger.error(colored("IOError: 获取分配预案" + str(e), "blue", attrs=['bold']))
    # except Exception as eb:
    #     logger.error(colored("获取现金流量: " + str(eb), "red", attrs=['bold']))


# 获取开始时间到现在所有的数据
getReport1("2001-01-01")

# sql = "select * from report where year = " + "2017" + " and quarter = " + "1";
# print sql
# hava_df = session.execute(sql)
# hava_df_list = list(hava_df)
# print len(hava_df_list)
# print type(hava_df)
# print hava_df


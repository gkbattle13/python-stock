#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
交易类数据提供股票的交易行情数据，主要包括以下类别:
    历史行情数据
    复权历史数据
    实时行情数据
    历史分笔数据
    实时报价数据
    当日历史分笔
    大盘指数列表
    大单交易数据
    Created on 2017-09-08
    @author: Guo Kun
    @contact: guokun@163.com
"""
import datetime
from sqlalchemy import create_engine
import tushare as ts
from sqlalchemy.orm import sessionmaker
from termcolor import *

import sys
# 防止报编码错误
reload(sys)
sys.setdefaultencoding('utf8')

engine = create_engine('mysql://wahaha:ceshi@106.14.238.126:3306/stock?charset=utf8');
Session = sessionmaker(bind=engine);
session = Session();

# 历史行情
# 获取个股历史交易数据（包括均线数据），可以通过参数设置获取日k线、周k线、月k线，以及5分钟、15
# 分钟、30分钟和60分钟k线数据。本接口只能获取近3年的日线数据，适合搭配均线数据进行选股和分析，
# 参数说明：
# code：股票代码，即6位数字代码，或者指数代码（sh=上证指数 sz=深圳成指 hs300=沪深300指数 sz50=上证50 zxb=中小板 cyb=创业板）
# start：开始日期，格式YYYY-MM-DD
# end：结束日期，格式YYYY-MM-DD
# ktype：数据类型，D=日k线 W=周 M=月 5=5分钟 15=15分钟 30=30分钟 60=60分钟，默认为D
# retry_count：当网络异常后重试次数，默认为3
# pause:重试时停顿秒数，默认为0
def hist_data(code):
    # 查询本地数据库，获取本地数据库中数据的最后获取日
    sql = "select date_add(max(date), interval 1 day)  as max_date from ts_deal_history where code ='" + code +"'";
    max_date = session.execute(sql)
    result =  max_date.first()
    # 判断本地数据库是否有数据
    if result["max_date"]:
        max_date_str = result["max_date"]
        today = datetime.date.today()
        lastday = max_date_str;
        print("数据库中存在code为" + code + "的交易数据，数据最后日期为：" + max_date_str.strftime('%Y-%m-%d') + " - 1天")
        # 判断如果本地数据日，小于当天日期，则去获取数据
        if (today - lastday).days > 1:
            df_report = ts.get_hist_data(code, start=max_date_str.strftime('%Y-%m-%d'))
            count = int(df_report.shape[0])
            if(count > 0 ) :
                print("获取到数据量为: " + str(df_report.shape[0]))
                df_report["code"] = code;
                df_report.to_sql("ts_deal_history", engine, if_exists="append");
                print("code: " + code + "新增数据入库成功")
            else:
                print("获取数据量为0,不做入库处理。")
        else:
            print("数据库中数据最后获取日期不小于今天，不做获取处理")
    else:
        # 如果本地数据库中不存在数据，调用函数获取最近三年数据
        print("数据库中不存在code为" + code + "的交易数据，开始获取")
        df_report = ts.get_hist_data(code)
        i = 0
        while i<3  and df_report is None:
            df_report = ts.get_hist_data(code)
            i += 1;
        else:
            print(colored("获取数据三次为空，暂时跳出", "red", attrs=['bold']))
            return code;
        print("获取到数据量为: " + str(df_report.shape[0]))
        df_report["code"] = code;
        df_report.to_sql("ts_deal_history", engine, if_exists="append");
        print("code: " + code + "新增数据入库成功")


# daef = hist_data("000002")

# 根据当天基础数据接口获取code集合，在循环获取历史行情
def getCurrentCode():
    s_basics = ts.get_stock_basics(); # 获取当天基础数据
    codeIndex = s_basics.index # 获取基础数据中code
    i = 0
    listFaild =[]
    for code in codeIndex:  # 遍历code获取历史行情
        code = hist_data(code);
        if code != None:
            listFaild.append(code)
        i+=1;
        print(colored("获取到第" + str(i), "blue", attrs=['bold']))
    print("获取失败：一共有" + str(len(listFaild)) + "条数据")
    print(listFaild)


# 更新单票据,数据库中已有数据往前1000天
def getHistory(code, market_date, days):
    # 获取数据库已有数据最早年份
    sql = "select date_add(min(date), interval -1 day)  as min_date from ts_deal_history where code ='" + code +"'";
    print(sql)
    min_date = session.execute(sql);
    result = min_date.first();

    # 判断本地数据库是否有数据
    if result["min_date"]:
        min_date = result["min_date"]
        befor_date = min_date + datetime.timedelta(days=days) # 获取1000天前的时间
        print("数据库中存在code为" + code + "的交易数据，数据最早日期为：" + (min_date + datetime.timedelta(days=1)).strftime('%Y-%m-%d'))

        # 判断如果本地数据日，小于当天日期，则去获取数据
        print(min_date)
        print(market_date)
        print(type(market_date))
        print (datetime.time.localtime(market_date))
        print(befor_date)
        if min_date > market_date & befor_date >= market_date :
            df_history = ts.get_h_data(code, start=befor_date.strftime('%Y-%m-%d'), end=min_date)
            count = int(df_history.shape[0])
            if(count > 0 ) :
                print("获取到数据量为: " + str(df_history.shape[0]))
                df_history["code"] = code;
                df_history.to_sql("ts_deal_history", engine, if_exists="append");
                print("code: " + code + "新增数据入库成功")
            else:
                print("获取数据量为0,不做入库处理。")
        else:
            print(colored("数据库中已有数据为最早日期为:" + min_date.strftime('%Y-%m-%d') + ", 上市日期为：" + market_date.strftime(
                '%Y-%m-%d') + "不再获取", "blue", attrs=['bold']))
    else:
        # 如果本地数据库中不存在数据，调用函数获取最近三年数据
        print("数据库中不存在code为" + code + "的交易数据，开始获取")
        df_report = ts.get_hist_data(code)
        i = 0
        while i<3  and df_report is None:
            df_report = ts.get_hist_data(code)
            i += 1;
        else:
            print(colored("获取数据三次为空，暂时跳出", "red", attrs=['bold']))
            return code;
        print("获取到数据量为: " + str(df_report.shape[0]))
        df_report["code"] = code;
        df_report.to_sql("ts_deal_history", engine, if_exists="append");
        print("code: " + code + "新增数据入库成功")


# 根据当天基础数据接口获取code集合，在循环获取历史行情 -- 3年前数据
def getHistoryCode():
    s_basics = ts.get_stock_basics();  # 获取当天基础数据
    codeIndex = s_basics.index  # 获取基础数据中code
    i = 0
    listFaild = []
    for code in codeIndex:  # 遍历code获取历史行情
        xx = s_basics.ix[code]['timeToMarket']
        code = getHistory(code,s_basics.ix[code]['timeToMarket'],1000 );
        if code != None:
            listFaild.append(code)
        i += 1;
        print(colored("获取到第" + str(i), "blue", attrs=['bold']))
    print("获取失败：一共有" + str(len(listFaild)) + "条数据")
    print(listFaild)


# 获取个股以往交易历史的分笔数据明细，通过分析分笔数据，可以大致判断资金的进出情况。
# 在使用过程中，对于获取股票某一阶段的历史分笔数据，需要通过参入交易日参数并append
# 到一个DataFrame或者直接append到本地同一个文件里。历史分笔接口只能获取当前交易日
# 之前的数据，当日分笔历史数据请调用get_today_ticks()接口或者在当日18点后通过本接口获取。
def get_tick_data():
    df = ts.get_tick_data('600848', date='2014-01-09')
    print(df)


getCurrentCode()
# getHistoryCode()


# sql = "select date_add(min(date), interval -1 day)  as min_date from ts_deal_history where code ='" + "000001" +"'";
# min_date = session.execute(sql)
# print type(min_date)
# result = min_date.first()
# print type(result["min_date"])
# date = result["min_date"] + datetime.timedelta(days=1)
# print type(date)
# print (result["min_date"] + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
# a = result["min_date"] > date
# print a

# hist_data("300703")

# today = datetime.date.today()
# yesterday = today - datetime.timedelta(days=1)
# a = today - yesterday
# print type(a)
# print a.days

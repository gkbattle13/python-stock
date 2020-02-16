# -*- coding: utf-8 -*-
import tushare_data as ts
from sqlalchemy import create_engine
import pandas as pd
import datetime

# 创建Mysql数据库连接
engine = create_engine('mysql://wahaha:ceshi@106.14.238.126:3306/stock?charset=utf8')

# 获取基本信息
def get_stock_basics():
    s_basics = ts.get_stock_basics();
    s_basics.to_sql("stock_basics_1", engine, if_exists = "append");
    print ("获取基本数据完毕一共获取数据: " + bytes(len(s_basics)));
    return len(s_basics);

# 获取某一条stock近三年的数据
def get_one_his(code=None, start=None, end=None):
    print ("开始获取：" + code + "的数据")
    df = ts.get_hist_data(code,start,end);
    df["code"] = code;
    df.to_sql('stock_history',engine,if_exists='append')
    print (" 代码：" + code + "近三年历史数据获取完毕。")

# 倒库
def get_stock_data():
    s_basics = get_stock_basics();
    for index,row in s_basics.iterrows():
        get_one_his(row.code);

get_stock_basics();
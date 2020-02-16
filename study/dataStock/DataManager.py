# -*- coding: utf-8 -*-
from sqlalchemy import create_engine
import tushare_data as ts
import pandas as pd
import datetime
import sys
import thread
reload(sys)
sys.setdefaultencoding('utf8')

# 创建Mysql数据库连接
engine = create_engine('mysql://wahaha:ceshi@106.14.238.126:3306/stock?charset=utf8')

# 创建数据库并插入数据
# df.to_sql('tick_history',engine)

# 获取单只demo最近三年的历史数据
# df = ts.get_hist_data('600848');
# df["tick_code"] = "600848";
# print (df);
# df.to_sql('tick_history',engine,if_exists='append')
# print ("over");


# 获取所有demo列表
# dsb = ts.get_stock_basics();
# dsb.to_sql('stock_basics',engine,if_exists='append');

sql = "select * from stock_basics order by code limit 0,100";
stock_basics = pd.read_sql(sql, engine);
# print (stock_basics);

# 获取基本信息
def get_stock_basics():
    s_basics = ts.get_stock_basics();
    s_basics.to_sql("stock_basics", engine, if_exists = "append");
    print ("获取基本数据完毕一共获取数据: " + bytes(len(s_basics)));
    return s_basics;

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

# 更新三年前的数据
def get_(code):
    # 获取基础表信息
    sql = "select * from stock_basics order by code limit 0,100";
    s_basics = pd.read_sql(sql,engine);
    i = 0;
    for index, row in s_basics.iterrows():
        startDate = row.timeToMarket; # 获取起始时间
        code = row.code; # 获取code
        print code + "上市日期为：" + str(startDate);
        i = i +1;
        print "获取到第：" + str(i) + "只 ";
        sql2 = "select min(date) as date from stock_history_2 where code = " + code;
        sql2Data = pd.read_sql(sql2, engine); # 获取目前数据库中code——history的最早时间
        if not pd.isnull(sql2Data).date[0]:
            hadBeginDate = sql2Data.date[0];
            datetime.datetime.strptime(str(timeToMarket), '%Y-%m-%d')
            datetime.datetime.strptime(str(hadBeginDate), '%Y-%m-%d')
            hadBeginDateOneYear = hadBeginDate - datetime.timedelta(days=365)
            while (hadBeginDate > timeToMarket):
                if (hadBeginDateOneYear > timeToMarket):
                    df = ts.get_h_data(code, start=hadBeginDateOneYear.strftime('%Y-%m-%d'),
                                       end=(hadBeginDate - datetime.timedelta(days=1)).strftime('%Y-%m-%d'),
                                       retry_count=10, pause=3)
                    df["code"] = code;
                    df.to_sql(tableName, engine, if_exists='append');
                    print " 获取" + code + "历史数据:" + hadBeginDateOneYear.strftime(
                        '%Y-%m-%d') + "  " + \
                          (hadBeginDate - datetime.timedelta(days=1)).strftime('%Y-%m-%d') + "保存数据库成功";
                    hadBeginDate = hadBeginDateOneYear;
                else:
                    df = ts.get_h_data(code, start=hadBeginDateOneYear.strftime('%Y-%m-%d'),
                                       end=(hadBeginDate - datetime.timedelta(days=1)).strftime('%Y-%m-%d'),
                                       retry_count=10, pause=3)
                    df["code"] = code;
                    df.to_sql(tableName, engine, if_exists='append');
                    print " 获取" + code + "历史数据:" + hadBeginDateOneYear.strftime(
                        '%Y-%m-%d') + "  " + \
                          (hadBeginDate - datetime.timedelta(days=1)).strftime('%Y-%m-%d') + "保存数据库成功, 该code获取所有历史数据结束";
                    hadBeginDate = hadBeginDateOneYear;
                    sql = "update stock_basics set isAll = 1 where code = " + code;
                    break;
            print   " 获取" + code + "历史数据:" + hadBeginDateOneYear.strftime('%Y-%m-%d') + \
                    "小于" + (hadBeginDate - datetime.timedelta(days=1)).strftime('%Y-%m-%d') + "不在获取";
        else:
            print "~~~~~~~~~~~~~代号:" + code + "在history表格中无数据存在"


def count():
    fs = []
    for i in range(1, 4):
        def f():
             return i*i
        fs.append(f)
    return fs;

f1, f2, f3 = count()
print f1();f2;f3;

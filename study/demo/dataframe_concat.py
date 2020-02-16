# -*- coding: utf-8 -*-
import time

import pandas as pd
import tushare_data as ts
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import sys
# 防止报编码错误
from data.caption.orm.Ts_Basic import Ts_Basic

reload(sys)

engine = create_engine('mysql://wahaha:ceshi@106.14.238.126:3306/stock?charset=utf8');
Session = sessionmaker(bind=engine);
session = Session();


sql = "select * from ts_basic" ;
pad123 = pd.read_sql(sql, con=engine)
print type(pad123)
pad123.set_index('code', inplace=True, drop=True)
indexp = pad123.index.values; # 取出index集合

list1 = [];
for a in indexp:
    cc = pad123.loc[a] # 根据index取dataframe里面的值
    ts_basic = Ts_Basic( code=a.encode('utf-8'), name=cc['name'].encode('utf-8'), industry=cc['industry'].encode('utf-8'),
              area=cc['area'].encode('utf-8'), pe=cc['pe'],
              outstanding=cc['outstanding'], totals=cc['totals'],
              total_assets=cc['total_assets'],liquid_assets=cc['liquid_assets'],
              fixed_assets=cc['fixed_assets'], reserved=cc['reserved'],
              reserved_per_share=cc['reserved_per_share'],esp=cc['esp'],
              bvps=cc['bvps'],pb=cc['pb'],time_to_market=cc['time_to_market'],
              undp=cc['undp'],perundp=cc['perundp'],rev=cc['rev'],
              profit=cc['profit'], gpr=cc['gpr'], npr=cc['npr'],
              holders=cc['holders'], update_time=cc['update_time'],
              is_over=cc['is_over'], data_loading=cc['data_loading'])
    list1.append(ts_basic)
    # print ts_basic
print len(list1)
# train_data = pd.np.array(pad123)#np.ndarray()
# train_x_list=train_data.tolist()#list
# print train_x_list
# print type(pad123)
# print pad123.shape
# print pad123.columns

df_basic = ts.get_stock_basics();
df_basic.rename(columns={'totalAssets':'total_assets', 'liquidAssets':'liquid_assets', 'fixedAssets':'fixed_assets',
                             'reservedPerShare': 'reserved_per_share', 'timeToMarket': 'time_to_market'}, inplace = True)
df_basic.insert(22,'update_time',time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
df_basic.insert(23,'is_over',0)
df_basic.insert(24,'data_loading',0)
print type(df_basic)
indexp2 = df_basic.index.values; # 取出index集合
list2 = [];
for a in indexp2:
    cc = df_basic.loc[a] # 根据index取dataframe里面的值
    ts_basic = Ts_Basic( code=a.encode('utf-8'), name=cc['name'], industry=cc['industry'],
              area=cc['area'], pe=cc['pe'],
              outstanding=cc['outstanding'], totals=cc['totals'],
              total_assets=cc['total_assets'],liquid_assets=cc['liquid_assets'],
              fixed_assets=cc['fixed_assets'], reserved=cc['reserved'],
              reserved_per_share=cc['reserved_per_share'],esp=cc['esp'],
              bvps=cc['bvps'],pb=cc['pb'],time_to_market=cc['time_to_market'],
              undp=cc['undp'],perundp=cc['perundp'],rev=cc['rev'],
              profit=cc['profit'], gpr=cc['gpr'], npr=cc['npr'],
              holders=cc['holders'], update_time=cc['update_time'],
              is_over=cc['is_over'], data_loading=cc['data_loading'])
    list2.append(ts_basic)
print len(list2)

i = 0
for x2 in list2:
    acc =  list1.index(x2)
    if(acc > 0):
        acc = acc+1
    print acc;
print i
# -*- coding: utf-8 -*-
import configparser
import inspect
import os
import sys
import tushare as ts
# 获取当前文件路径
current_path = inspect.getfile(inspect.currentframe())
# 获取当前文件所在目录，相当于当前文件的父目录
dir_name = os.path.dirname(current_path)
# 转换为绝对路径
file_abs_path = os.path.abspath(dir_name)
# 划分目录，比如a/b/c划分后变为a/b和c
list_path = os.path.split(file_abs_path)
print("添加包路径为：" + list_path[0])
sys.path.append(list_path[0])
from tushare_data.utils import loggerUtils
from sqlalchemy import create_engine


# 获取SQL, ThShare, Log
def sql_tuShare_log(config_name):
    cp = configparser.ConfigParser()
    cp.read(file_abs_path + "/" + config_name)
    print("读取配置文件路径：" + file_abs_path + "/config.ini")
    # 获取thshare api
    tushare_token = cp.get("tushare", "tushare_token")
    ts.set_token(tushare_token)
    pro = ts.pro_api()

    # 获取日志记录对象
    logger = loggerUtils.TNLog()

    # 获取mysql链接
    db_host = cp.get("db", "db_host")
    db_user = cp.get("db", "db_user")
    db_pass = cp.get("db", "db_pass")
    engine = create_engine('mysql+pymysql://' + db_user + ':' + db_pass + '@' + db_host + '/stock?charset=UTF8MB4',
                           encoding='utf-8', pool_size=30, max_overflow=30)
    # python 2.7用法
    # engine = create_engine('mysql://root:rootroot@localhost:3306/stock?charset=utf8', pool_size=30, max_overflow=30)
    return engine, pro, logger



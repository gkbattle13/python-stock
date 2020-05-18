import configparser
import os
import tushare as ts

from tushare_data.utils import loggerUtils
from sqlalchemy import create_engine


# 获取SQL, ThShare, Log
def sql_tuShare_log():
    cp = configparser.ConfigParser()
    path1 = os.path.abspath('.')
    cp.read(path1+"/config.ini")

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


# 获取配置文件
def get_config(section, option):
    cp = configparser.ConfigParser()
    cp.read("config.ini")
    return cp.get(section, option)

sql_tuShare_log()
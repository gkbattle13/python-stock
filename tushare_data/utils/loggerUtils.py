#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
该日志类可以把不同级别的日志输出到不同的日志文件中,文件为根目录下文件
'''
import os
import time
import logging
import inspect

from pandas import DataFrame
from sqlalchemy import create_engine

file = "/Users/guo/Code/3. GIt/financial"

handlers = {logging.DEBUG: file + "/log-debug.log",
            logging.INFO: file + "/log-info.log",
            logging.WARNING: file + "/log-warning.log",
            logging.ERROR: file + "/log-error.log",
            logging.CRITICAL: file + "/log-critical.log"}


def createHandlers():
    logLevels = handlers.keys()
    for level in logLevels:
        path = os.path.abspath(handlers[level])
        handlers[level] = logging.FileHandler(filename=path, encoding="UTF-8")


# 加载模块时创建全局变量
createHandlers()


class TNLog(object):
    def printfNow(self):
        return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

    def __init__(self, level=logging.NOTSET):
        self.__loggers = {}
        logLevels = handlers.keys()
        for level in logLevels:
            logger = logging.getLogger(str(level))
            # 如果不指定level，获得的handler似乎是同一个handler?
            logger.addHandler(handlers[level])
            logger.setLevel(level)
            self.__loggers.update({level: logger})

    def getLogMessage(self, level, message):
        frame, filename, lineNo, functionName, code, unknowField = inspect.stack()[2]
        '''日志格式：[时间] [类型] [记录代码] 信息'''
        return "[%s] [%s] [%s - %s - %s] %s" % (self.printfNow(), level, filename, lineNo, functionName, message)

    def info(self, message):
        message = self.getLogMessage("info", message)
        print(message)
        self.__loggers[logging.INFO].info(message)

    def error(self, message):
        message = self.getLogMessage("error", message)
        print(message)
        self.__loggers[logging.ERROR].error(message)

    def warning(self, message):
        message = self.getLogMessage("warning", message)
        print(message)
        self.__loggers[logging.WARNING].warning(message)

    def debug(self, message):
        message = self.getLogMessage("debug", message)
        print(message)
        self.__loggers[logging.DEBUG].debug(message)

    def critical(self, message):
        message = self.getLogMessage("critical", message)
        print(message)
        self.__loggers[logging.CRITICAL].critical(message)

    # 用于在保存后记录出错的数据
    def errorlog(self, fun_name=None, ts_code=None, trade_date=None, start_date=None, end_date=None, cal_date=None,
                 time_num=None, e=None):
        engine = create_engine('mysql://root:rootroot@localhost:3306/stock?charset=utf8')
        d2 = DataFrame({
            'fun_name': [str(fun_name)],
            'ts_code': [str(ts_code)],
            'trade_date': [str(trade_date)],
            'start_date': [str(start_date)],
            'end_date': [str(end_date)],
            'cal_date': [str(cal_date)],
            'error_info': [str(e)],
            'create_time': [time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())],
            'time': [time_num]
        })
        d2.to_sql("error_daily", engine, if_exists="append", index=False)


if __name__ == "__main__":
    logger = TNLog()
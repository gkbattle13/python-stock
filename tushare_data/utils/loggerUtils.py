#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
该日志类可以把不同级别的日志输出到不同的日志文件中,文件为根目录下文件
'''
import time
import logging
import inspect
import os

from pandas import DataFrame

# 获取当前文件路径
current_path = inspect.getfile(inspect.currentframe())
# 获取当前文件所在目录，相当于当前文件的父目录
dir_name = os.path.dirname(current_path)
# 转换为绝对路径
file_abs_path = os.path.abspath(dir_name)
# 划分目录，比如a/b/c划分后变为a/b和c
list_path = os.path.split(file_abs_path)
list_path1 = os.path.split(list_path[0])
print("添加包路径为：" + list_path1[0])
file = list_path1[0] + "/log"
handlers = {logging.DEBUG: file + "/debug-" + time.strftime("%Y%m%d", time.localtime()) + ".log",
            logging.INFO: file + "/info-" + time.strftime("%Y%m%d", time.localtime()) + ".log",
            logging.WARNING: file + "/warning-" + time.strftime("%Y%m%d", time.localtime()) + ".log",
            logging.ERROR: file + "/error-" + time.strftime("%Y%m%d", time.localtime()) + ".log",
            logging.CRITICAL: file + "/critical-" + time.strftime("%Y%m%d", time.localtime()) + ".log"}


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
    def errorMysqlLog(self, engine, fun_name=None, ts_code=None, trade_date=None, start_date=None, end_date=None,
                      cal_date=None,
                      time_num=None, e=None):
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

    # 通用记录方法
    def infoMysql(self, engine, full_name=None, fun_name=None, parameter=None, status=None, error_info=None,
                  result_count=None):
        a = "获取成功，" if status == 1 else "获取失败，"
        description = full_name + a + "参数为：" + parameter
        self.insertInfoDaily(engine=engine, full_name=full_name, fun_name=fun_name, parameter=parameter, status=status,
                             error_info=error_info, result_count=result_count, description=description)
        logger.info(description)

    # 写入mysql Info_daily
    def insertInfoDaily(self, engine, full_name=None, fun_name=None, parameter=None, status=None, error_info=None,
                        result_count=None, description=None):
        d2 = DataFrame({
            'full_name': [str(full_name)],
            'fun_name': [str(fun_name)],
            'parameter': [str(parameter)],
            'status': [str(status)],
            'error_info': [str(error_info)],
            'result_count': [str(result_count)],
            'description': [str(description)],
            'create_time': [time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())],
        })
        d2.to_sql("info_daily", engine, if_exists="append", index=False)


if __name__ == "__main__":
    logger = TNLog()

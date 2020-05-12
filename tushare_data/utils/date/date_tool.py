#!/usr/bin/env python
# -*- coding: utf-8 -*-

import calendar
import datetime
from datetime import datetime
import random
import time
import pandas as pd


def getBetweenDay(begin_date):
    date_list = []
    begin_date = datetime.datetime.strptime(begin_date, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(time.strftime('%Y-%m-%d', time.localtime(time.time())), "%Y-%m-%d")
    while begin_date <= end_date:
        date_str = begin_date.strftime("%Y-%m")
        date_list.append(date_str)
        begin_date = add_months(begin_date, 1)
    return date_list


def add_months(dt, months):
    month = dt.month - 1 + months
    year = dt.year + month / 12
    month = month % 12 + 1
    day = min(dt.day, calendar.monthrange(int(year), month)[1])
    return dt.replace(year=int(year), month=month, day=day)


# 获取所有月，返回一个列表:
def getBetweenMonth(begin_date, end_date):
    # 返回所有月份，以及每月的起始日期、结束日期，字典格式
    date_list = {}
    begin_date = datetime.datetime.strptime(begin_date, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    while begin_date <= end_date:
        date_str = begin_date.strftime("%Y-%m")
        date_list[date_str] = ['%d-%d-01' % (begin_date.year, begin_date.month),
                               '%d-%d-%d' % (begin_date.year, begin_date.month,
                                             calendar.monthrange(begin_date.year, begin_date.month)[1])]
        begin_date = add_months(begin_date, 1)
    return date_list


# 获取所有季度，返回一个列表
# 获取所有天，返回一个列表:
def getBetweenQuarter(begin_date):
    quarter_list = []
    month_list = getBetweenDay(begin_date)
    for value in month_list:
        tempvalue = value.split("-")
        if tempvalue[1] in ['01', '02', '03']:
            quarter_list.append(tempvalue[0] + "Q1")
        elif tempvalue[1] in ['04', '05', '06']:
            quarter_list.append(tempvalue[0] + "Q2")
        elif tempvalue[1] in ['07', '08', '09']:
            quarter_list.append(tempvalue[0] + "Q3")
        elif tempvalue[1] in ['10', '11', '12']:
            quarter_list.append(tempvalue[0] + "Q4")
    quarter_set = set(quarter_list)
    quarter_list = list(quarter_set)
    quarter_list.sort()
    return quarter_list


# 获取一个随机数
def getRomandId():
    return time.strftime("%Y%m%d%H%M%S", time.localtime()) + str(random.randint(100000, 999999));


"""
根据起始时间和结束时间获取一个日期集合, 包头包尾
end_date：可以为空，默认为当天
format：需要的日期格式，默认为：%Y-%m-%d
"""


def getBetweenDayList(begin_date, end_date=None, format=None):
    date_list = []

    begin_date = datetime.datetime.strptime(begin_date, "%Y-%m-%d")
    if end_date is None:
        end_date = datetime.datetime.strptime(time.strftime('%Y-%m-%d', time.localtime(time.time())), "%Y-%m-%d")
    else:
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    if format is None:
        format = "%Y-%m-%d"
    while begin_date <= end_date:
        date_str = begin_date.strftime(format)
        date_list.append(date_str)
        begin_date += datetime.timedelta(days=1)
    return date_list


def getLastDayOfMonth(beginDate, endDate):
    date_index = pd.date_range(beginDate, endDate)
    days = [pd.Timestamp(x).strftime("%Y-%m-%d") for x in date_index.values]

    tmp = []
    for index, v in enumerate(days):
        if index == len(days) - 1:
            tmp.append(days[index])
        # if index == 0:
        # tmp.append(days[0])
        else:
            _ = v.split('-')[2]
            if _ == '01':
                tmp.append(days[index - 1])
                tmp.append(days[index])
    return tmp


# 返回yyyyMMdd日期格式是周几
def changeToWeek(date_2):
    try:
        week = datetime.strptime(date_2, "%Y%m%d").weekday()
        return week
    except Exception as e:
        print("获取周几失败" + e.message)

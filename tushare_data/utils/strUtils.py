#!/usr/bin/env python
# -*- coding: utf-8 -*-

import chardet


def noneToUndecided(str):
    if str is None:
        str = "未定义"
    return str


def merge(dict1, dict2):
    return dict2.update(dict1)


def toDictionary(*args):
    dict = {}
    for arg in args:
        dict = merge(dict, arg)
    return dict


# 猜编码
def char():
    f = open("unknown.txt", "r")
    fstr = f.read()
    print(chardet.detect(fstr))

    # 输出
    # {'confidence': XXX, 'encoding': 'XXX'}

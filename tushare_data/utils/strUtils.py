#!/usr/bin/env python
# -*- coding: utf-8 -*-

import chardet

def noneToWdy(str):
    if(str is None):
        str = "未定义"
    return str


# 猜编码
def char():
    f = open("unknown.txt","r")
    fstr = f.read()
    print(chardet.detect(fstr))

    # 输出
    # {'confidence': XXX, 'encoding': 'XXX'}
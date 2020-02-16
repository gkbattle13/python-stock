#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
第1行和第2行是标准注释
第1行注释可以让这个hello.py文件直接在Unix/Linux/Mac上运行
第2行注释表示.py文件本身使用标准UTF-8编码；
第4行开始是一个字符串，表示模块的文档注释，任何模块代码的第一个字符串都被视为模块的文档注释；
Created on 2015年6月10日
@author: Guo Kun
@group : waditu
@contact: jimmysoa@sina.cn
"""

__author__ = 'Michael Liao'

import sys
# 比如Python标准库一般会提供StringIO和cStringIO两个库，这两个库的接口和功能是一样的，但是cStringIO是C写的，速度更快
try:
    import cStringIO as StringIO
except ImportError: # 导入失败会捕获到ImportError
    import StringIO

try:
    import json # python >= 2.6
except ImportError:
    import simplejson as json # python <= 2.5

"""
 由于Python是由社区推动的开源并且免费的开发语言，不受商业公司控制，
 因此，Python的改进往往比较激进，不兼容的情况时有发生。
 Python为了确保你能顺利过渡到新版本，特别提供了__future__模块，让你在旧的版本中试验新版本的一些特性。
"""
from __future__ import unicode_literals

_private_1=""; #为私有变量，不应该被引用

def test():
    args = sys.argv
    if len(args)==1:
        print 'Hello, world!'
    elif len(args)==2:
        print 'Hello, %s!' % args[1]
    else:
        print 'Too many arguments!'

if __name__=='__main__':
    test()
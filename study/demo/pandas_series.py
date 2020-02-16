# -*- coding: utf-8 -*-

import pandas as pd
from pandas import Series, DataFrame, np

"""
Series:
Series是一个一维的类似的数组对象，包含一个数组的数据（任何NumPy的数据类型）和一个与数组关联的数据标签，被叫做 索引 。
最简单的Series是由一个数组的数据构成
"""

# 创建Series（不带索引）因为我们没有给数据指定索引，一个包含整数0到N-1（这里N是数据的长度）的默认索引被创建
obj = Series([4, 7, -5, 3])
print obj.values; # 取值
print obj.index;  # 取索引
print obj;

# 创建带有索引的Series
obj2 = Series([4, 7, -5, 3], index=['a', 'b', 'c', 'd'])
print obj2.index
print obj2
print obj2 ['a'] # 通过索引取值
print obj2 [['a','b']] # 通过索引取值

# NumPy数组操作，例如通过一个布尔数组过滤，纯量乘法，或使用数学函数，将会保持索引和值间的关联
print obj2 [obj2 > 2]
print obj2 * 2;
print np.exp(obj2);

# 另一种思考的方式是，Series是一个定长的，有序的字典，因为它把索引和值映射起来了。它可以适用于许多期望一个字典的函数
print 'b' in obj2;
print 4 in obj2;

# python 字典转换为Series
sdata = {'Ohio': 35000, 'Texas': 71000, 'Oregon': 16000, 'Utah': 5000}
obj3 = Series(sdata);
print obj3

# 使用其他索引替换原索引，查找不到时候为Non
states = ['California', 'Ohio', 'Oregon', 'Texas']
obj4 = Series(sdata, index=states)
print obj4

# 判断数据丢失
print pd.isnull(obj4)
print pd.notnull(obj4)

# 只需要保证索引名称相同，在运算时会数据会自动对齐
print obj3
print obj4
print obj3 + obj4

# Series 本身和他的Index都有一个name属性
obj4.name = "obj4_name"
obj4.index.name = "obj4_index_name"
print obj4.name
print obj4.index.name
print obj4

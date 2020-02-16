# -*- coding: utf-8 -*-

import pandas as pd
from pandas import Series, DataFrame, np

"""
pandas的索引对象用来保存坐标轴标签和其它元数据（如坐标轴名或名称）。
构建一个Series或DataFrame时任何数组或其它序列标签在内部转化为索引
"""
obj = Series(range(3), index=['a', 'b', 'c'])
index = obj.index
print index
print index[1:]

# 索引对象是不可变的，因此不能由用户改变：
# index[1] = 'd'
# 索引对象的不可变性非常重要，这样它可以在数据结构中结构中安全的共享：
index = pd.Index(np.arange(3))
obj2 = Series([1.5, -2.5, 0], index=index)
print obj2.index is index

"""
            pandas中的主要索引对象
Index	        最通用的索引对象，使用Python对象的NumPy数组来表示坐标轴标签。
Int64Index	    对整形值的特化索引。
MultiIndex	    “分层”索引对象，表示单个轴的多层次的索引。可以被认为是类似的元组的数组。
DatetimeIndex	存储纳秒时间戳（使用NumPy的datetime64 dtyppe来表示）。
PeriodIndex	    对周期数据（时间间隔的）的特化索引。
"""
pop = {'Nevada': {2001: 2.4, 2002: 2.9},'Ohio': {2000: 1.5, 2001: 1.7, 2002: 3.6}}
frame3 = DataFrame(pop)
print frame3
print 'Ohio' in frame3.columns
print 2003 in frame3.index

# 每个索引都有许多关于集合逻辑的方法和属性，且能够解决它所包含的数据的常见问题
"""
            索引方法和属性
append	        链接额外的索引对象，产生一个新的索引
diff	        计算索引的差集
intersection	计算交集
union	        计算并集
isin	        计算出一个布尔数组表示每一个值是否包含在所传递的集合里
delete	        计算删除位置i的元素的索引
drop	        计算删除所传递的值后的索引
insert	        计算在位置i插入元素后的索引
is_monotonic	返回True，如果每一个元素都比它前面的元素大或相等
is_unique	    返回True，如果索引没有重复的值
unique	        计算索引的唯一值数组
"""

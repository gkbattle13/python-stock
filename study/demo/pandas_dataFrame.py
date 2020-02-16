# -*- coding: utf-8 -*-
import uuid

import pandas as pd
import ts
from pandas import Series, DataFrame, np
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

"""
DataFrame:
一个Datarame表示一个表格，类似电子表格的数据结构，包含一个经过排序的列表集，
它们没一个都可以有不同的类型值（数字，字符串，布尔等等）。Datarame有行和列的索引；
它可以被看作是一个Series的字典（每个Series共享一个索引）。与其它你以前使用过的（如 R 的 data.frame )
类似Datarame的结构相比，在DataFrame里的面向行和面向列的操作大致是对称的。
在底层，数据是作为一个或多个二维数组存储的，而不是列表，字典，或其它一维的数组集合。
DataDrame内部的精确细节已超出了本书的范围。因为DataFrame在内部把数据存储为一个二维数组的格式，
因此你可以采用分层索引以表格格式来表示高维的数据。分层索引是后面章节的一个主题，
并且是pandas中许多更先进的数据处理功能的关键因素。
"""
# 列表转换为DataFrame 由此产生的索引由系统自己分配 并且对列进行了排序
data = {'state': ['Ohio', 'Ohio', 'Ohio', 'Nevada', 'Nevada'],
        'year': [2000, 2001, 2002, 2001, 2002],
        'pop': [1.5, 1.7, 3.6, 2.4, 2.9]}
frame = DataFrame(data)
print frame

# 如果你设定了列的顺序，那么会按照你对列的顺序排序
frame1 = DataFrame(data, columns=['year', 'state', 'pop'])
print frame1

# 如果一个行中没有值，一样用NoN表示
frame2 = DataFrame(data, columns=['year', 'state', 'pop', 'debt'],
                   index=['one', 'two', 'three', 'four', 'five'])
print frame2
print frame2.columns

# 通过字典或属性可以取值
print frame2.year
print frame2['state']

# 行也可以使用一些方法通过位置或名字来检索，例如 ix 索引成员（field）
print frame2.ix['three']
# 注意，返回的Series包含和DataFrame相同的索引，并它们的 name 属性也被正确的设置了

# 列可以通过赋值来修改。例如，空的 ‘debt’ 列可以通过一个纯量或一个数组来赋值
frame2.debt = '0';
print frame2

# 通过列表或数组给一列赋值时，所赋的值的长度必须和DataFrame的长度相匹配。
# 如果你使用Series来赋值，它会代替在DataFrame中精确匹配的索引的值，并在说有的空洞插入丢失数据
val = Series([-1.2, -1.5, -1.7], index=['two', 'four', 'five'])
frame2['debt'] = val
print frame2

# 给一个不存在的列赋值，将会创建一个新的列。 像字典一样 del 关键字将会删除列
frame2['eastern'] = frame2.state == 'Ohio'
print frame2
del frame2['eastern']
print frame2.columns

# 索引DataFrame时返回的列是底层数据的一个视窗，而不是一个拷贝。
# 因此，任何在Series上的就地修改都会影响DataFrame。列可以使用Series的 copy 函数来显式的拷贝。
fram = frame2.copy()
print fram

# 另一种通用的数据形式是一个嵌套的字典的字典格式：
# 如果被传递到DataFrame，它的外部键会被解释为列索引，内部键会被解释为行索引：
pop = {'Nevada': {2001: 2.4, 2002: 2.9},'Ohio': {2000: 1.5, 2001: 1.7, 2002: 3.6}}
frame3 = DataFrame(pop)
print frame3

# 行列互转
frame3 = frame3.T
print frame3

# 内部字典的键被结合并排序来形成结果的索引。如果指定了一个特定的索引，就不是这样的了
frame3 = DataFrame(pop, index=[2001, 2002, 2003])
print frame3

# 属性
frame3.index.name = 'year';
frame3.columns.name = 'state'
frame3.values
# 获取所有index的值 <type 'numpy.ndarray'>
indexp = frame3.index.values;

# 重新命名columns
a = pd.DataFrame({'A':[1,2,3], 'B':[4,5,6], 'C':[7,8,9]})
a.columns = ['a','b','c']  # 暴力方法  麻烦：全部命名都得写上去
a.rename(columns={'A':'a', 'C':'c'}, inplace = True) #  比较好的方法：需要改什么就写什么

# 重新设置 index
# inplace，如果设置为True就不会返回一个新的DataFrame，而是直接修改该DataFrame
# drop，如果设置为True，就会移出掉该列的数据
a.set_index('c', inplace=True, drop=True)

# 去除一列
series1 = a.pop('a') #取出来的列为Series
# 添加一列
a.insert(0,'series1',series1) #参数为： columns位置从0开始，列名，添加的数据内容
a['winter'] = uuid.uuid1()  # 直接赋值也可以
print a;

# 获取pd大小
print a.shape

# 按照索引去除对应的行
o = a.loc[1]
p = a.loc[1:3]

# 现在我要删除索引号为1和2的这两行
fandango_drop = p.drop([1,2], axis=0)
# 看到了吧，iloc[2]的意思是选择第三行的数据，也就是索引号为4的那一行数据，
# 因为iloc[]的计算也是从0开始的，所以iloc[]适用于数据进行了筛选后造成索引号与原来不一致的情况
#
# loc[]与iloc[]方法之间还有一个巨大的差别，那就是loc[]里的参数是对应的索引值即可，
# 所以参数可以是整数，也可以是字符串。而iloc[]里的参数表示的是第几行的数据，所以只能是整数

"""
                        可能的传递到DataFrame的构造器
二维ndarray	                一个数据矩阵，有可选的行标和列标
数组，列表或元组的字典	    每一个序列成为DataFrame中的一列。所有的序列必须有相同的长度。
NumPy的结构/记录数组	        和“数组字典”一样处理
Series的字典	                每一个值成为一列。如果没有明显的传递索引，将结合每一个Series的索引来形成结果的行索引。
字典的字典	                每一个内部的字典成为一列。和“Series的字典”一样，结合键值来形成行索引。
字典或Series的列表	        每一项成为DataFrame中的一列。结合字典键或Series索引形成DataFrame的列标。
列表或元组的列表	            和“二维ndarray”一样处理
另一个DataFrame	            DataFrame的索引将被使用，除非传递另外一个
NumPy伪装数组（MaskedArray）	除了蒙蔽值在DataFrame中成为NA/丢失数据之外，其它的和“二维ndarray”一样
"""
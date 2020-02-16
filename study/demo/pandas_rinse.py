# -*- coding: utf-8 -*-



# 合并两个DataFrame
from pandas import merge

merge(df_mysql, df_mysql, how='inner', on=None, left_on=None, right_on=None,
      left_index=False, right_index=False, sort=True,
      suffixes=('_x', '_y'), copy=True, indicator=False)
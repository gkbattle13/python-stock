# 获取基础数据
import inspect
import os
import sys
import time

# 获取当前文件路径
current_path = inspect.getfile(inspect.currentframe())
# 获取当前文件所在目录，相当于当前文件的父目录
dir_name = os.path.dirname(current_path)
# 转换为绝对路径
file_abs_path = os.path.abspath(dir_name)
# 划分目录，比如a/b/c划分后变为a/b和c
list_path = os.path.split(file_abs_path)
print("添加包路径为：" + list_path[0])
sys.path.append(list_path[0])

from tushare_data import configuration
from tushare_data.data.box2 import basic
from tushare_data.data.box2 import market_data
from tushare_data.data.box2 import fund
from tushare_data.data.box2 import market_reference_resources


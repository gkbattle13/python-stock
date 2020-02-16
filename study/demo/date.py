# 导入MySQL驱动:
import mysql.connector
# 注意把password设为你的root口令:
import pandas as pd

conn = mysql.connector.connect(user='root', password='password', database='test', use_unicode=True)
cursor = conn.cursor()
# 创建user表:
cursor.execute('create table user (id varchar(20) primary key, name varchar(20))')
# 插入一行记录，注意MySQL的占位符是%s:
cursor.execute('insert into user (id, name) values (%s, %s)', ['1', 'Michael'])
cursor.rowcount
1
# 提交事务:
conn.commit()
cursor.close()
# 运行查询:
cursor = conn.cursor()
cursor.execute('select * from user where id = %s', ('1',))
values = cursor.fetchall()
values
[(u'1', u'Michael')]
# 关闭Cursor和Connection:
cursor.close()
True
conn.close()



# pandas
"""
read_sql

参见pandas.read_sql的文档，read_sql主要有如下几个参数：
sql:SQL命令字符串
con：连接sql数据库的engine，一般可以用SQLalchemy或者pymysql之类的包建立
index_col: 选择某一列作为index
coerce_float:非常有用，将数字形式的字符串直接以float型读入
parse_dates:将某一列日期型字符串转换为datetime型数据，与pd.to_datetime函数功能类似。可以直接提供需要转换的列名以默认的日期形式转换，也可以用字典的格式提供列名和转换的日期格式，比如{column_name: format string}（format string："%Y:%m:%H:%M:%S"）。
columns:要选取的列。一般没啥用，因为在sql命令里面一般就指定要选择的列了
chunksize：如果提供了一个整数值，那么就会返回一个generator，每次输出的行数就是提供的值的大小。
params：其他的一些执行参数，没用过不太清楚。。。


to_sql

参见pandas.to_sql函数，主要有以下几个参数：
name: 输出的表名
con: 与read_sql中相同
if_exits： 三个模式：fail，若表存在，则不输出；replace：若表存在，覆盖原来表里的数据；append：若表存在，将数据写到原表的后面。默认为fail
index：是否将df的index单独写到一列中
index_label:指定列作为df的index输出，此时index为True
chunksize： 同read_sql
dtype: 指定列的输出到数据库中的数据类型。字典形式储存：{column_name: sql_dtype}。常见的数据类型有sqlalchemy.types.INTEGER(), sqlalchemy.types.NVARCHAR(),sqlalchemy.Datetime()等，具体数据类型可以参考这里
还是以写到mysql数据库为例：

df.to_sql(name='table', 
      con=con, 
      if_exists='append', 
      index=False,
      dtype={'col1':sqlalchemy.types.INTEGER(),
             'col2':sqlalchemy.types.NVARCHAR(length=255),
             'col_time':sqlalchemy.DateTime(),
             'col_bool':sqlalchemy.types.Boolean
      })
"""
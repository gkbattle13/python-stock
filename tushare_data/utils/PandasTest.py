# -*- coding:utf-8 -*-

import pandas as pd


# 读取数据
def test1():
    sql = "select * from fund_nav limit 0,1000"
    df = pd.read_sql(sql, mySQL.db)

    # 读取csv数据
    # pd.read_csv()

    # 读取excel数据
    # pd.read_excel()
    # 读取txt数据
    # pd.read_table()
    print(df.columns)
    return df


def main():
    global mySQL
    mySQL = PyMySqlUtils.utils()
    mySQL._init_('10.0.11.21', 'root', 'root@2014', 'Test')
    test1()


if __name__ == "__main__":
    main()

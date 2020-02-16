from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

engine = create_engine('mysql://wahaha:ceshi@106.14.238.126:3306/stock?charset=utf8');
Session = sessionmaker(bind=engine);
session = Session();

sql = "select date_add(max(date), interval 1 day)  as max_date from ts_deal_history where code ='" + "000001" + "'";
print sql


# 返回类型为ResultProxy
max_date = session.execute(sql)

# 取出类型为'sqlalchemy.engine.result.RowProxy'
rows = max_date.fetchall() # 取出所有行
rowfirst = max_date.first();  #取出第一行
rowfirst["code"] # 取出对应值


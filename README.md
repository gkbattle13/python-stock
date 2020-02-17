# python-stock
  通过tushare接口获取金融数据
tushare 官网及注册地址: 
    https://tushare.pro/register?reg=124747 


1. 项目主要通过python调用tushare接口获取金融原始数据
2. 需要在tushare官网注册用户获取token，替换tushare_data下Test2020中ts.set_token值
3. 替换tushare_data下Test2020中mysql配置，mysql入库时会自动创建表，所以无需初始化sql
4. 直接执行tushare_data下Test2020可以获取对应的数据

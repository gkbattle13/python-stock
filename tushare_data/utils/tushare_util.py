import time

from tushare_data.utils import strUtils


class tushare_util():

    # 初始化: 数据连接：(engine), tushare api：(pro), 日志：（logger）
    def __init__(self, engine, pro, logger):
        self.engine = engine
        self.pro = pro
        self.logger = logger

    # 通用方法调用tushare接口
    def common_interface(self, fun_name, table_name, info, filed, **key):
        try:
            data = self.pro.query(api_name=fun_name, fields=filed, **key)
            data.to_sql(table_name, self.engine, if_exists="append", index=False)
            self.logger.info(info + "，  参数： " + str(key) + " ,  数据量： " + str(len(data)) + "成功")
        except Exception as e:
            self.logger.error(info + "： " + str(e))

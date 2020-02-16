#!/usr/bin/env python
# -*- coding: utf-8 -*-


class  MarketAll():

    def __init__(self, engine, pro, logger):
        self.engine = engine
        self.pro = pro
        self.logger = logger

    """
    指数基本信息获取
    MSCI	MSCI指数
    CSI	中证指数
    SSE	上交所指数
    SZSE	深交所指数
    CICC	中金所指数
    SW	申万指数
    CNI	国证指数
    OTH	其他指数
    一次获取以上所有的指数基本数据
    """
    def index_basic(self):
        self.get_index_basic("MSCI")
        self.get_index_basic("CSI")
        self.get_index_basic("SSE")
        self.get_index_basic("SZSE")
        self.get_index_basic("CICC")
        self.get_index_basic("SW")
        self.get_index_basic("CNI")
        self.get_index_basic("OTH")

    # 根据参数获取指数基本信息
    def get_index_basic(self, market, publisher = None, category = None ):
        try:
            data = self.pro.index_basic(market = market, publisher = publisher,  category = category)
            data.set_index("ts_code", inplace=True, drop=True)
            data.to_sql("index_basic", self.engine, if_exists="append")
            info = 'TuShare 指数基本信息获取 market:'+ market + ', 数据共：' + str(len(data)) + "条数据"
            self.loggerInfo(info)
        except Exception as e:
            error = "TuShare 指数基本信息获取 market: " + market + str(e)
            self.loggerError(error)


    """
    指数日线行情:获取指数每日行情，还可以通过bar接口获取。由于服务器压力，目前规则是单次调取最多取2800行记录，
    可以设置start和end日期补全。
    """


    def loggerInfo(self,info):
        self.logger.info(str(info))
        print(str(info))

    def loggerError(self,error):
        self.logger.error(str(error))
        print(str(error))

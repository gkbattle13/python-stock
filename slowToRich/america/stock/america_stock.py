import akshare as ak
import pandas as pd
import efinance as ef

from tushare_data.utils import strUtils


class america_stock():

    # 初始化: 数据连接：(engine), tushare api：(pro), 日志：（logger）
    def __init__(self, engine, pro, logger):
        self.engine = engine
        self.pro = pro
        self.logger = logger

    def get_us_stock_name(self):
        try:
            data = ak.get_us_stock_name()
            data.to_sql("stock_us_name", self.engine, index=False)
        except Exception as e:
            print("ERROR" + e)
            self.logger.infoMysql(engine=self.engine, full_name=None, fun_name="ggt_top10",
                                  parameter=None,
                                  status=0, error_info=str(e), result_count=None)

    def get_us_stock_name_from_mysql(self):
        try:
            sql = "select symbol from stock_us_name"
            concept = pd.read_sql(sql, self.engine)
                # try:
                #     self.engine.execute("TRUNCATE reference_concept_detail")
                # except Exception as e:
                #     print("ERROR" + str(e))

            df1 = pd.DataFrame(
                columns=["symbol", "amplitude3", "amplitude5", "amplitude10", "max3", "min3", "max5", "min5",
                         "max10", "min10"])
            i = 0
            for index, row in concept.iterrows():
                try:
                    # ef 包获取
                    # data = ef.stock.get_quote_history(stock_codes=strUtils.noneToUndecided(row),beg ='20150101')

                    # ak 包获取
                    data = ak.stock_us_fundamental(stock=strUtils.noneToUndecided(row.symbol)) # 基本面
                    data1 = ak.stock_us_daily(symbol=strUtils.noneToUndecided(row.symbol))     # 历史数据

                    i += 1
                    # 计算3年
                    max3 = data1.tail(650)["high"].max()
                    min3 = data1.tail(650)["high"].min()

                    # 计算5年
                    max5 = data1.tail(1075)["high"].max()
                    min5 = data1.tail(1075)["high"].min()

                    # 计算10年
                    max10 = data1.tail(2150)["high"].max()
                    min10 = data1.tail(2150)["high"].min()

                    df1.loc[i] = [row.symbol, round(max3/min3,2),  round(max5/min5,2),  round(max10/min10,2),
                                  max3, min3, max5,min5, max10, min10]
                    # data1[["index","high"]].groupby(by='',as.index=)


                    # max = print(data1["high"].max())
                    # aa = data1["high"].iloc[max,:]
                    # a = data1.max("high")/data1.min("low")
                    # print(strUtils.noneToUndecided(row.symbol) + a)
                    # data.to_sql("stock_us_fundamental", self.engine,if_exists="append", index=False)
                    # data1.to_sql("stock_us_daily", self.engine, if_exists="append", index=True)
                except Exception as e:
                    print("ERROR" + str(e))
            # 入库
            df1.to_sql("stock_us_amplitude", self.engine, if_exists="append", index=True)
        except Exception as e:
            print("ERROR" + str(e))

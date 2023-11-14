import time
import akshare as ak
import pandas as pd
import threadpool as threadpool

from concurrent.futures import ThreadPoolExecutor
from tushare_data.utils import strUtils

""" 通过AKShare 接口获取Am数据 https://www.akshare.xyz/data/stock/stock.html#id41 """


class america_stock():

    # 初始化: 数据连接：(engine), tushare api：(pro), 日志：（logger）
    def __init__(self, engine, pro, logger):
        self.engine = engine
        self.pro = pro
        self.logger = logger

    """获取美股所有公司名单"""

    def get_us_stock_name(self):
        try:
            data = ak.get_us_stock_name()
            data.to_sql("stock_us_name", self.engine, index=False)
        except Exception as e:
            print("ERROR" + str(e))

    """百度指数"""

    def baidu(self):
        try:
            # 谷歌指数
            data = ak.baidu_search_index()  # 获取百度搜索指数
            print(data)
            # data.to_sql("baidu_search_index", self.engine, index=False)

            data = ak.baidu_info_index()  # 获取百度资讯指数
            print(data)
            # data.to_sql("baidu_info_index", self.engine, index=False)

            data = ak.baidu_media_index()  # 获取百度媒体指数
            print(data)
            # data.to_sql("baidu_media_index", self.engine, index=False)

            print("获取百度指数完成")
        except Exception as e:
            print("ERROR" + str(e))

    """Google 指数"""
    def google(self):
        """谷歌指数"""
        try:
            data = ak.google_index()  # 谷歌指数
            data.to_sql("google_index", self.engine, index=False)
            print("获取google指数完成")
        except Exception as e:
            print("ERROR" + str(e))

    # 获取3个月，1，3，5，10年上涨倍率
    def get_stock_us_daily(self):

        pool = threadpool.ThreadPool(10)

        # 1. 获取名称
        sql = "select symbol,cname from stock_us_famous_spot_em "
        # sql = "select symbol,cname from stock_us_famous_spot_em limit 6336, 17000"
        # sql = "select symbol,cname from stock_us_amplitude_error where reason = 'ERROR'"
        concept = pd.read_sql(sql, self.engine)
        # try:
        #     self.engine.execute("TRUNCATE reference_concept_detail")
        # except Exception as e:
        #     print("ERROR" + str(e))

        # 2. 获取整体数据
        df1 = pd.DataFrame(
            columns=["symbol", "cname", "recent_price", "recent_time",
                     "amplitude3m_recentMin", "amplitude3m_recentMax", "min_time3m", "max_time3m",
                     "amplitude6m_recentMin", "amplitude6m_recentMax", "min_time6m", "max_time6m",
                     "amplitude1_recentMin", "amplitude1_recentMax", "min_time1", "max_time1",

                     "amplitude3m", "amplitude1", "amplitude3", "amplitude5", "amplitude10", "amplitude",
                     "min3m", "max3m", "min6m", "max6m", "min1", "max1",
                     "min_time3", "min3", "max_time3", "max3",
                     "min_time5", "min5", "max_time5", "max5",
                     "min_time10", "min10", "max_time10", "max10",
                     "min_time", "min", "max_time", "max"])

        dfError = pd.DataFrame(columns=["symbol", "cname", "reason", "time", "price"])

        # 3. 计算倍率
        i = 0
        for index, row in concept.iterrows():
            try:
                # ef 包获取
                # data = ef.stock.get_quote_history(stock_codes=strUtils.noneToUndecided(row),beg ='20150101')

                # ak 包获取
                data = ak.stock_us_daily(symbol=strUtils.noneToUndecided(row.symbol), adjust="qfq")  # 历史数据
                # data = ak.stock_us_daily(symbol=strUtils.noneToUndecided("OPK"), adjust="qfq")  # 历史数据
                data = data.drop(data[(data['high'] == 0) | (data['volume'] == 0)].index)

                # 计算15天
                data15d = data.tail(11).sort_values(ascending=False, by='high')
                listIndex15d = list(data15d.index)

                # 计算1个月
                data1m = data.tail(21).sort_values(ascending=False, by='high')
                listIndex1m = list(data1m.index)

                # 计算3个月
                data3m = data.tail(64).sort_values(ascending=False, by='high')
                listIndex3m = list(data3m.index)

                # 如果获取到的数据为0跳过计算
                if data3m.iloc[1, 1] == 0 or int((time.time() - data.index[-1].value / 1000000000)) > 86400 * 10:
                    dfError.loc[0] = (str(row.symbol), row.cname, "Recent None", listIndex3m[1], data3m.iloc[1, 1])
                    print("ERROR       Symbol:  " + str(row.symbol), "   CName:  ", row.cname, "reason: ",
                          "Recent None")
                    print("ERROR       Symbol:  " + str(row.symbol), "  price: ", data3m.iloc[1, 1], " Time: ",
                          listIndex3m[1])
                    try:
                        dfError.to_sql("stock_us_amplitude_error", self.engine, if_exists="append", index=False)
                    except Exception as e:
                        print("ERROR  TO Mysql  Code:  " + str(row.symbol), "   cname:  ", row.cname, str(e))
                    finally:
                        dfError.drop(df1.index, inplace=True)
                    continue

                # 计算6个月
                data6m = data.tail(128).sort_values(ascending=False, by='high')
                listIndex6m = list(data6m.index)

                # 计算1年
                data1 = data.tail(254).sort_values(ascending=False, by='high')
                listIndex1 = list(data1.index)

                # 计算3年
                data3 = data.tail(760).sort_values(ascending=False, by='high')
                listIndex3 = list(data3.index)

                # 计算5年
                data5 = data.tail(1267).sort_values(ascending=False, by='high')
                listIndex5 = list(data5.index)

                # 计算10年
                data10 = data.tail(2534).sort_values(ascending=False, by='high')
                listIndex10 = list(data10.index)

                # 计算整体
                dataMax = data.sort_values(ascending=False, by='high')
                listIndex = list(dataMax.index)

                # 获取正负
                amplitude15d = 0
                amplitude1m = 0
                amplitude3m = 0
                amplitude1 = 0
                amplitude3 = 0
                amplitude5 = 0
                amplitude10 = 0
                amplitudeMax = 0
                if amplitude15d[0].value > amplitude15d[-1].value:
                    amplitude15d = round((data15d.iloc[0, 1] / data15d.iloc[-1, 1] - 1) * 100, 2)
                else:
                    amplitude15d = - round(((data15d.iloc[0, 1] - data15d.iloc[-1, 1]) / data15d.iloc[0, 1]) * 100, 2)

                if listIndex1m[0].value > listIndex1m[-1].value:
                    amplitude1m = round((data1m.iloc[0, 1] / data1m.iloc[-1, 1] - 1) * 100, 2)
                else:
                    amplitude1m = - round(((data1m.iloc[0, 1] - data1m.iloc[-1, 1]) / data1m.iloc[0, 1]) * 100, 2)

                if listIndex3m[0].value > listIndex3m[-1].value:
                    amplitude3m = round((data3m.iloc[0, 1] / data3m.iloc[-1, 1] - 1) * 100, 2)
                else:
                    amplitude3m = - round(((data3m.iloc[0, 1] - data3m.iloc[-1, 1]) / data3m.iloc[0, 1]) * 100, 2)

                if listIndex1[0].value > listIndex1[-1].value:
                    amplitude1 = round((data1.iloc[0, 1] / data1.iloc[-1, 1] - 1) * 100, 2)
                else:
                    amplitude1 = - round(((data1.iloc[0, 1] - data1.iloc[-1, 1]) / data1.iloc[0, 1]) * 100, 2)

                if listIndex3[0].value > listIndex3[-1].value:
                    amplitude3 = round((data3.iloc[0, 1] / data3.iloc[-1, 1] - 1) * 100, 2)
                else:
                    amplitude3 = - round(((data3.iloc[0, 1] - data3.iloc[-1, 1]) / data3.iloc[-1, 1]) * 100, 2)

                if listIndex5[0].value > listIndex5[-1].value:
                    amplitude5 = round((data5.iloc[0, 1] / data5.iloc[-1, 1] - 1) * 100, 2)
                else:
                    amplitude5 = - round(((data5.iloc[0, 1] - data5.iloc[-1, 1]) / data5.iloc[-1, 1]) * 100, 2)

                if listIndex10[0].value > listIndex10[-1].value:
                    amplitude10 = round((data10.iloc[0, 1] / data10.iloc[-1, 1] - 1) * 100, 2)
                else:
                    amplitude10 = - round(((data10.iloc[0, 1] - data10.iloc[-1, 1]) / data10.iloc[-1, 1]) * 100,
                                          2)

                if listIndex[0].value > listIndex[-1].value:
                    amplitudeMax = round((dataMax.iloc[0, 1] / dataMax.iloc[-1, 1] - 1) * 100, 2)
                else:
                    amplitudeMax = - round(
                        ((dataMax.iloc[0, 1] - dataMax.iloc[-1, 1]) / dataMax.iloc[-1, 1]) * 100, 2)

                # 计算与当前的比例
                amplitude3m_recentMax = 0
                amplitude3m_recentMin = 0
                amplitude6m_recentMax = 0
                amplitude6m_recentMin = 0
                amplitude1_recentMax = 0
                amplitude1_recentMin = 0

                recent_time = list(data.index)[-1]
                recent_price = data.iloc[-1, 1]

                if recent_price == data3m.iloc[0, 1]:
                    amplitude3m_recentMax = 10000
                else:
                    amplitude3m_recentMax = -round(((data3m.iloc[0, 1] - recent_price) / data3m.iloc[0, 1]) * 100,
                                                   2)

                if recent_price == data3m.iloc[-1, 1]:
                    amplitude3m_recentMin = -10000
                else:
                    amplitude3m_recentMin = round(((recent_price - data3m.iloc[-1, 1]) / data3m.iloc[-1, 1]) * 100,
                                                  2)

                if recent_price == data6m.iloc[0, 1]:
                    amplitude6m_recentMax = 10000
                else:
                    amplitude6m_recentMax = -round(((data6m.iloc[0, 1] - recent_price) / data6m.iloc[0, 1]) * 100,
                                                   2)

                if recent_price == data6m.iloc[-1, 1]:
                    amplitude6m_recentMin = -10000
                else:
                    amplitude6m_recentMin = round(((recent_price - data6m.iloc[-1, 1]) / data6m.iloc[-1, 1]) * 100,
                                                  2)

                if recent_price == data1.iloc[0, 1]:
                    amplitude1_recentMax = 10000
                else:
                    amplitude1_recentMax = -round(((data1.iloc[0, 1] - recent_price) / data1.iloc[0, 1]) * 100,
                                                  2)

                if recent_price == data1.iloc[-1, 1]:
                    amplitude1_recentMin = -10000
                else:
                    amplitude1_recentMin = round(((recent_price - data1.iloc[-1, 1]) / data1.iloc[-1, 1]) * 100,
                                                 2)

                df1.loc[i] = [row.symbol, row.cname,
                              recent_price, recent_time,
                              amplitude3m_recentMin, amplitude3m_recentMax, listIndex3m[-1], listIndex3m[0],
                              amplitude6m_recentMin, amplitude6m_recentMax, listIndex6m[-1], listIndex6m[0],
                              amplitude1_recentMin, amplitude1_recentMax, listIndex1[-1], listIndex1[0],

                              amplitude3m, amplitude1, amplitude3, amplitude5, amplitude10, amplitudeMax,

                              data3m.iloc[-1, 1], data3m.iloc[0, 1], data6m.iloc[-1, 1], data6m.iloc[0, 1],
                              data1.iloc[-1, 1], data1.iloc[0, 1],
                              listIndex3[-1], data3.iloc[-1, 1], listIndex3[0], data3.iloc[0, 1],
                              listIndex5[-1], data5.iloc[-1, 1], listIndex5[0], data5.iloc[0, 1],
                              listIndex10[-1], data10.iloc[-1, 1], listIndex10[0], data10.iloc[0, 1],
                              listIndex[-1], dataMax.iloc[-1, 1], listIndex[0], dataMax.iloc[0, 1]]
                i += 1

                # if df1.size % 100 == 0:
                #     df1.to_sql("stock_us_amplitude", self.engine, if_exists="append", index=False)
                #
                df1.to_sql("stock_us_amplitude", self.engine, if_exists="append", index=False)
                df1.drop(df1.index, inplace=True)

                data = None
                dataMax = None
                data10 = None

                print("Code: ", i, "  CName: ", row.symbol, " Symbol: ", row.cname)
            except Exception as e:
                try:
                    dfError.loc[0] = (str(row.symbol), row.cname, "ERROR")
                    dfError.to_sql("stock_us_amplitude_error", self.engine, if_exists="append", index=False)
                except Exception as e:
                    print("ERROR  TO Mysql  Code:  " + str(row.symbol), "   cname:  ", row.cname, str(e))
                finally:
                    df1.drop(df1.index, inplace=True)
                    dfError.drop(df1.index, inplace=True)
                    # print("ERROR" + str(e))

    """ 多线程获取美股数据Call """

    def get_stock_us_daily_sigel_thread(self, symbol_i, cname_i):
        dfError = pd.DataFrame(columns=["symbol", "cname", "reason", "time", "price"])
        # 3. 计算倍率
        try:
            # ak 包获取
            data = ak.stock_us_daily(symbol=strUtils.noneToUndecided(symbol_i), adjust="qfq")  # 历史数据
            data = data.drop(data[(data['high'] == 0) | (data['volume'] == 0)].index)

            # 计算3个月
            data3m = data.tail(64).sort_values(ascending=False, by='high')
            listIndex3m = list(data3m.index)

            # 如果获取到的数据为0跳过计算
            # if data3m.iloc[1, 1] == 0 or int((time.time() - data.index[-1] / 1000000000)) > 86400 * 10:
            if data3m.iloc[1, 1] == 0:
                dfError.loc[0] = (str(symbol_i), cname_i, "Recent None", listIndex3m[1], data3m.iloc[1, 1])
                print(" !!!!!!!!!!!!!!!! ERROR  不合格数据  Symbol:  " + str(symbol_i), "   CName:  ", cname_i, "reason: ",
                      "Recent None", "price: ", data3m.iloc[1, 1], " Time: ", listIndex3m[1])
                try:
                    dfError.to_sql("stock_us_amplitude_error", self.engine, if_exists="append", index=False)
                except Exception as e:
                    print(" !!!!!!!!!!!!!!!! ERROR  TO Mysql  Code:  " + str(symbol_i), "   cname:  ", cname_i, str(e))
                return

            # 计算6个月
            data6m = data.tail(128).sort_values(ascending=False, by='high')
            listIndex6m = list(data6m.index)

            # 计算1年
            data1 = data.tail(254).sort_values(ascending=False, by='high')
            listIndex1 = list(data1.index)

            # 计算3年
            data3 = data.tail(760).sort_values(ascending=False, by='high')
            listIndex3 = list(data3.index)

            # 计算5年
            data5 = data.tail(1267).sort_values(ascending=False, by='high')
            listIndex5 = list(data5.index)

            # 计算10年
            data10 = data.tail(2534).sort_values(ascending=False, by='high')
            listIndex10 = list(data10.index)

            # 计算整体
            dataMax = data.sort_values(ascending=False, by='high')
            listIndex = list(dataMax.index)

            # 获取正负
            amplitude3m = 0
            amplitude1 = 0
            amplitude3 = 0
            amplitude5 = 0
            amplitude10 = 0
            amplitudeMax = 0
            if listIndex3m[0] > listIndex3m[-1]:
                amplitude3m = round((data3m.iloc[0, 1] / data3m.iloc[-1, 1] - 1) * 100, 2)
            else:
                amplitude3m = - round(((data3m.iloc[0, 1] - data3m.iloc[-1, 1]) / data3m.iloc[0, 1]) * 100, 2)

            if listIndex1[0] > listIndex1[-1]:
                amplitude1 = round((data1.iloc[0, 1] / data1.iloc[-1, 1] - 1) * 100, 2)
            else:
                amplitude1 = - round(((data1.iloc[0, 1] - data1.iloc[-1, 1]) / data1.iloc[0, 1]) * 100, 2)

            if listIndex3[0] > listIndex3[-1]:
                amplitude3 = round((data3.iloc[0, 1] / data3.iloc[-1, 1] - 1) * 100, 2)
            else:
                amplitude3 = - round(((data3.iloc[0, 1] - data3.iloc[-1, 1]) / data3.iloc[-1, 1]) * 100, 2)

            if listIndex5[0] > listIndex5[-1]:
                amplitude5 = round((data5.iloc[0, 1] / data5.iloc[-1, 1] - 1) * 100, 2)
            else:
                amplitude5 = - round(((data5.iloc[0, 1] - data5.iloc[-1, 1]) / data5.iloc[-1, 1]) * 100, 2)

            if listIndex10[0] > listIndex10[-1]:
                amplitude10 = round((data10.iloc[0, 1] / data10.iloc[-1, 1] - 1) * 100, 2)
            else:
                amplitude10 = - round(((data10.iloc[0, 1] - data10.iloc[-1, 1]) / data10.iloc[-1, 1]) * 100,
                                      2)

            if listIndex[0] > listIndex[-1]:
                amplitudeMax = round((dataMax.iloc[0, 1] / dataMax.iloc[-1, 1] - 1) * 100, 2)
            else:
                amplitudeMax = - round(
                    ((dataMax.iloc[0, 1] - dataMax.iloc[-1, 1]) / dataMax.iloc[-1, 1]) * 100, 2)

            # 计算与当前的比例
            amplitude3m_recentMax = 0
            amplitude3m_recentMin = 0
            amplitude6m_recentMax = 0
            amplitude6m_recentMin = 0
            amplitude1_recentMax = 0
            amplitude1_recentMin = 0

            recent_time = list(data.index)[-1]
            recent_price = data.iloc[-1, 1]

            if recent_price == data3m.iloc[0, 1]:
                amplitude3m_recentMax = 10000
            else:
                amplitude3m_recentMax = -round(((data3m.iloc[0, 1] - recent_price) / data3m.iloc[0, 1]) * 100,
                                               2)

            if recent_price == data3m.iloc[-1, 1]:
                amplitude3m_recentMin = -10000
            else:
                amplitude3m_recentMin = round(((recent_price - data3m.iloc[-1, 1]) / data3m.iloc[-1, 1]) * 100,
                                              2)

            if recent_price == data6m.iloc[0, 1]:
                amplitude6m_recentMax = 10000
            else:
                amplitude6m_recentMax = -round(((data6m.iloc[0, 1] - recent_price) / data6m.iloc[0, 1]) * 100,
                                               2)

            if recent_price == data6m.iloc[-1, 1]:
                amplitude6m_recentMin = -10000
            else:
                amplitude6m_recentMin = round(((recent_price - data6m.iloc[-1, 1]) / data6m.iloc[-1, 1]) * 100,
                                              2)

            if recent_price == data1.iloc[0, 1]:
                amplitude1_recentMax = 10000
            else:
                amplitude1_recentMax = -round(((data1.iloc[0, 1] - recent_price) / data1.iloc[0, 1]) * 100,
                                              2)

            if recent_price == data1.iloc[-1, 1]:
                amplitude1_recentMin = -10000
            else:
                amplitude1_recentMin = round(((recent_price - data1.iloc[-1, 1]) / data1.iloc[-1, 1]) * 100,
                                             2)

            print(" GET   CName: ", cname_i, " Symbol: ", symbol_i, " Success")

            return [symbol_i, cname_i,
                    recent_price, recent_time,
                    amplitude3m_recentMin, amplitude3m_recentMax, listIndex3m[-1], listIndex3m[0],
                    amplitude6m_recentMin, amplitude6m_recentMax, listIndex6m[-1], listIndex6m[0],
                    amplitude1_recentMin, amplitude1_recentMax, listIndex1[-1], listIndex1[0],
                    amplitude3m, amplitude1, amplitude3, amplitude5, amplitude10, amplitudeMax,
                    data3m.iloc[-1, 1], data3m.iloc[0, 1], data6m.iloc[-1, 1], data6m.iloc[0, 1],
                    data1.iloc[-1, 1], data1.iloc[0, 1],
                    listIndex3[-1], data3.iloc[-1, 1], listIndex3[0], data3.iloc[0, 1],
                    listIndex5[-1], data5.iloc[-1, 1], listIndex5[0], data5.iloc[0, 1],
                    listIndex10[-1], data10.iloc[-1, 1], listIndex10[0], data10.iloc[0, 1],
                    listIndex[-1], dataMax.iloc[-1, 1], listIndex[0], dataMax.iloc[0, 1]]
        except Exception as e:
            try:
                print(" !!!!!!!!!!!!!!!! ERROR  Code:  " + str(symbol_i), "   cname:  ", cname_i, " ERROR: " + str(e))
                dfError.loc[0] = (str(symbol_i), cname_i, str(e))
                dfError.to_sql("stock_us_amplitude_error", self.engine, if_exists="append", index=False)
            except Exception as e:
                print(" !!!!!!!!!!!!!!!! ERROR TO Save Error Mysql  Code:  " + str(symbol_i), "   cname:  ", cname_i, str(e))

    # 获取3个月，1，3，5，10年上涨倍率 多线程主线程
    def get_stock_us_daily_thread(self):
        start_time = time.time()
        df1 = pd.DataFrame(
            columns=["symbol", "cname", "recent_price", "recent_time", "amplitude3m_recentMin", "amplitude3m_recentMax",
                     "min_time3m", "max_time3m", "amplitude6m_recentMin", "amplitude6m_recentMax", "min_time6m",
                     "max_time6m", "amplitude1_recentMin", "amplitude1_recentMax", "min_time1", "max_time1",
                     "amplitude3m", "amplitude1", "amplitude3", "amplitude5", "amplitude10", "amplitude", "min3m",
                     "max3m", "min6m", "max6m", "min1", "max1", "min_time3", "min3", "max_time3", "max3", "min_time5",
                     "min5", "max_time5", "max5", "min_time10", "min10", "max_time10", "max10", "min_time", "min",
                     "max_time", "max"])

        # 1. 获取名称
        sql = " select symbol,cname from stock_us_famous_spot_em order by symbol limit 3000, 2000 "
        concept = pd.read_sql(sql, self.engine)

        # 2. 参数转换
        symbol_i = concept['symbol'].tolist()
        cname_i = concept['cname'].tolist()

        # 3. 开启多线程
        executor = ThreadPoolExecutor(max_workers=6)

        i = 1
        for result in executor.map(self.get_stock_us_daily_sigel_thread, symbol_i, cname_i):
            # print("task{}:{}".format(i, result))
            df1.loc[i] = result
            print(" 获取序列号：", i)
            # if i % 300 == 0:
            # 4. 保存Mysql
            # try:
            #     result.to_sql("stock_us_amplitude", self.engine, if_exists="append", index=False)
            #     df1.to_sql("stock_us_amplitude", self.engine, if_exists="append", index=False)
            #     df1.drop(df1.index, inplace=True)
            #     print(" !!!!!!!!!!!!!!!!   Save TO  Mysql  size : ", df1.size)
            #     print("task{}".format(i))
            #
            # except Exception as e:
            #     print(" !!!!!!!!!!!!!!!!   ERROR TO Save  Mysql ", str(e))

            i += 1

        try:
            df1.to_sql("stock_us_amplitude", self.engine, if_exists="append", index=False)
            df1.drop(df1.index, inplace=True)
            print(" Save TO  Mysql  size : ", len(df1))
            end_time = time.time()
            print("耗时: {:.2f}秒".format(end_time - start_time))
        except Exception as e:
            print(" !!!!!!!!!!!!!!!! ERROR TO Save Error Mysql  Code:  " + str(symbol_i), "   cname:  ", cname_i,
                  str(e))
        finally:
            exit()

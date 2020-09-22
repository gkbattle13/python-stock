from tushare_data.utils import strUtils, tushare_util
import pandas as pd


class alternative():

    # 初始化: 数据连接：(engine), tushare api：(pro), 日志：（logger）
    def __init__(self, engine, pro, logger):
        self.engine = engine
        self.pro = pro
        self.logger = logger

    """
    根据输入的日期循环获取一段时间内日线行情的数据 循环调用news接口,使用trade_date参数，可能有些股票现在不存在
    """

    def news_cycle(self, exchange=None, start_date=None, end_date=None, src=None):
        sql = 'select * from basic_trade_cal where 1=1 '
        if not exchange is None:
            sql = sql + ' and exchange = "' + exchange + '"'
        if not start_date is None:
            sql = sql + ' and cal_date >= ' + start_date
        if not end_date is None:
            sql = sql + ' and cal_date <= ' + end_date
        data_1 = pd.read_sql(sql, self.engine)
        for index, row in data_1.iterrows():
            date_2 = row["cal_date"]
            # date_2.decode('utf8')
            date_new = date_2[0:4] + "-" + date_2[4:6] + "-" + date_2[-2:]
            start_date_new = date_new + " 00:00:00"
            end_date_new = date_new + " 24:00:00"
            self.news(start_date=start_date_new,end_date = end_date_new, src=src)

    """
    接口：news
    描述：新闻快讯
    积分：获取主流新闻网站的快讯新闻数据,单次最大1000条新闻
    start_date	datetime	Y	开始日期
    end_date	datetime	Y	结束日期
    src	str	Y	新闻来源 见下表
    来源名称	src标识	描述
    新浪财经	sina	获取新浪财经实时资讯
    华尔街见闻	wallstreetcn	华尔街见闻快讯
    同花顺	10jqka	同花顺财经新闻
    东方财富	eastmoney	东方财富财经新闻
    云财经	yuncaijing	云财经新闻
    """

    def news(self, start_date=None, end_date=None, src=None):
        # info = "TuShare 另类数据 新闻快讯"
        # tushare_util_entry = tushare_util.tushare_util(self.engine, self.pro, self.logger)
        # tushare_util_entry.common_interface(fun_name='news', table_name="test_1", info="TuShare 另类数据 新闻快讯", filed="",
        #                                     ts_code='000001.SZ')  # 基金持股变化

        try:
            data = self.pro.news(start_date=start_date, end_date=end_date, src=src)
            data.to_sql("alternative_news", self.engine, if_exists="append", index=False)
            self.logger.info(info + strUtils.noneToUndecided(start_date) + " ,  数据量： " + str(len(data)) + "成功")
        except Exception as e:
            self.logger.error(info + "数据：" + str(e))

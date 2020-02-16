#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import time
import pandas as pd
import time


# 获取市场数据
class MarketReferenceData:

    def __init__(self, engine, pro, logger):
        self.engine = engine
        self.pro = pro
        self.logger = logger

    #  沪深港通资金流向, 获取开始时间到结束时间，或者某一天的 沪深港通资金流向
    def ts_moneyflow_hsgt(self, trade_date=None, startDate=None, endDate=None):
        try:
            data = self.pro.moneyflow_hsgt(start_date=startDate, end_date=endDate)
            data.set_index("trade_date", inplace=True, drop=True)
            data.to_sql("market_reference_moneyflow_hsgt", self.engine, if_exists="append")
            info = 'TuShare 沪深港通资金流向获取 trade_date:' + trade_date + '; startDate: ' + startDate + '; endDate: ' + endDate + '. 数据共：' + str(
                len(data)) + "条数据"
            self.logger.info(info)
        except Exception as e:
            self.errorData('ts_moneyflow_hsgt', cal_date=trade_date, start_date=startDate, end_date=endDate,
                           error_info=e)
            error = 'TuShare 沪深港通资金流向获取 trade_date:' + trade_date + '; startDate: ' + startDate + '; endDate: ' + endDate + '. 数据失败' + str(
                e)
            self.logger.error(error)

    # 获取沪股通、深股通每日前十大成交详细数据
    # ts_code	    str	N	股票代码（二选一）
    # trade_date	str	N	交易日期（二选一）
    # start_date	str	N	开始日期
    # end_date	    str	N	结束日期
    # market_type	str	N	市场类型（1：沪市 3：深市）
    def ts_hsgt_top10(self, ts_code=None, trade_date=None, start_date=None, end_date=None, market_type=None):
        try:
            data = self.pro.hsgt_top10(ts_code=ts_code, trade_date=trade_date, start_date=start_date, end_date=end_date,
                                       market_type=market_type)
            data.set_index("trade_date")
            data.to_sql('market_hsgt_top10', self.engine, if_exists="append")
            info = 'TuShare 沪深股通十大成交股获取 : ts_code: ' + ts_code + '  trade_date:' + trade_date + '; startDate: ' + start_date + '; endDate: ' + end_date + '. 数据共：' + str(
                len(data)) + "条数据"
            self.logger.info(info)
        except Exception as e:
            self.errorData('ts_moneyflow_hsgt', cal_date=trade_date, start_date=start_date, end_date=end_date,
                           error_info=e)
            error = 'TuShare 沪深港通资金流向获取 trade_date:' + trade_date + '; startDate: ' + start_date + '; endDate: ' + end_date + '. 数据失败' + str(
                e)
            self.logger.error(error)

    # 获取港股通每日成交数据，其中包括胡沪市、深市详细数据
    # ts_code	    str	N	股票代码（二选一）
    # trade_date	str	N	交易日期（二选一）
    # start_date	str	N	开始日期
    # end_date	    str	N	结束日期
    # market_type	str	N	市场类型 2：港股通（沪） 4：港股通（深）
    def ggt_top10(self, ts_code=None, trade_date=None, start_date=None, end_date=None, market_type=None):
        try:
            data = self.pro.ggt_top10(ts_code=ts_code, trade_date=trade_date, start_date=start_date, end_date=end_date,
                                      market_type=market_type)
            data.set_index("ts_code")
            data.to_sql("market_ggt_top10", self.engine, if_exists="append")
            self.logger.info(
                "TuShare 港股通十大成交股: ts_code" + ts_code + ' trade_date:' + trade_date + '; startDate: ' + start_date + '; endDate: ' + end_date + '. 数据共：' + str(
                    len(data)) + "条数据")
        except Exception as e:
            self.errorData('ggt_top10', ts_code=ts_code, cal_date=trade_date, start_date=start_date, end_date=end_date,
                           error_info=e)
            error = "TuShare 港股通十大成交股: ts_code" + ts_code + ' trade_date:' + trade_date + '; startDate: ' + start_date + '; endDate: ' + end_date + '. 数据失败' + str(
                e)
            self.logger.error(error)

    # 融资融券交易汇总
    # trade_date	str	Y	交易日期
    # exchange_id	str	N	交易所代码
    def margin(self, trade_date, exchange_id=None):
        try:
            data = self.pro.margin(trade_date=trade_date, exchange_id=exchange_id)
            data.set_index("ts_code")
            data.to_sql("market_margin", self.engine, if_exists="append")
            self.logger.info(
                "TuShare 融资融券交易汇总: trade_date: " + trade_date + ' exchange_id:' + exchange_id + '. 数据共：' + str(
                    len(data)) + "条数据")
        except Exception as e:
            self.errorData('margin', cal_date=trade_date, error_info=e)
            error = "TuShare 融资融券交易汇总: trade_date: " + trade_date + ' exchange_id:' + exchange_id + '. 数据失败' + str(e)
            self.logger.error(error)

    # 获取沪深两市每日融资融券明细
    # trade_date	str	Y	交易日期
    # exchange_id	str	N	交易所代码
    def margin_detail(self, trade_date, exchange_id=None):
        try:
            data = self.pro.margin_detail(trade_date=trade_date, exchange_id=exchange_id)
            data.set_index("ts_code")
            data.to_sql("market_margin_detail", self.engine, if_exists="append")
            self.logger.info(
                "TuShare 融资融券交易明细: trade_date: " + trade_date + ' exchange_id:' + exchange_id + '. 数据共：' + str(
                    len(data)) + "条数据")
        except Exception as e:
            self.errorData('margin_detail', cal_date=trade_date, error_info=e)
            error = "TuShare 融资融券交易明细: trade_date: " + trade_date + ' exchange_id:' + exchange_id + '. 数据失败: ' + str(e)
            self.logger.error(error)

    # 前十大股东
    # ts_code	    str	Y	TS代码
    # end_date	    str	N	报告期top10_holders  // 参数冲突
    # ann_date	    str	N	公告日期
    # strat_date	str	N	报告期开始日期
    # end_date	    str	N	报告期结束日期
    def top10_holders(self, ts_code, ann_date=None, strat_date=None, end_date=None):
        try:
            data = self.pro.top10_holders(ts_code=ts_code, ann_date=ann_date, strat_date=strat_date, end_date=end_date)
            data.set_index("ts_code")
            data.to_sql("market_top10_holders", self.engine, if_exists="append")
            self.logger.info(
                "TuShare 前十大股东: ts_code: " + ts_code + ' ann_date:' + ann_date + ' strat_date:' + strat_date + ' end_date:' + end_date + '. 数据共：' + str(
                    len(data)) + "条数据")
        except Exception as e:
            self.errorData('top10_holders', ts_code=ts_code, start_date=strat_date, end_date=end_date, error_info=e)
            error = "TuShare 前十大股东: ts_code: " + ts_code + ' ann_date:' + ann_date + ' strat_date:' + strat_date + ' end_date:' + end_date + '. 数据失败: ' + str(
                e)
            self.logger.error(error)

    # 前十大流通股东
    # ts_code	    str	Y	TS代码
    # end_date	    str	N	报告期top10_holders  // 参数冲突
    # ann_date	    str	N	公告日期
    # strat_date	str	N	报告期开始日期
    # end_date	    str	N	报告期结束日期
    def top10_floatholders(self, ts_code, ann_date=None, start_date=None, end_date=None):
        try:
            data = self.pro.top10_floatholders(ts_code=ts_code, ann_date=ann_date, strat_date=start_date,
                                               end_date=end_date)
            data.set_index("ts_code")
            data.to_sql("market_top10_floatholders", self.engine, if_exists="append")
            self.logger.info(
                "TuShare 前十大流通股东: ts_code: " + ts_code + ' ann_date:' + ann_date + ' strat_date:' + start_date + ' end_date:' + end_date + '. 数据共：' + str(
                    len(data)) + "条数据")
        except Exception as e:
            self.errorData('top10_floatholders', ts_code=ts_code, start_date=start_date, end_date=end_date,
                           error_info=e)
            error = "TuShare 前十大流通股东: ts_code: " + ts_code + ' ann_date:' + ann_date + ' strat_date:' + start_date + ' end_date:' + end_date + '. 数据失败: ' + str(
                e)
            self.logger.error(error)

    # 插入error表
    def errorData(self, fun_name=None, ts_code=None, cal_date=None, start_date=None, end_date=None, error_info=None):
        d2 = pd.DataFrame({
            'fun_name': [str(fun_name)],
            'ts_code': [str(ts_code)],
            'cal_date': [str(cal_date)],
            'start_date': [str(start_date)],
            'end_date': [str(end_date)],
            'create_time': [str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))],
            'time': [1],
            'error_info': [str(error_info)]
        })
        d2.to_sql("error_daily", self.engine, if_exists="append")

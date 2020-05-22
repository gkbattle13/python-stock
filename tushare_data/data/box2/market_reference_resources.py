import time

import pandas as pd
import sqlalchemy

# 市场参考数据
from tushare_data.utils import strUtils


class market_reference_resources():

    # 初始化: 数据连接：(engine), tushare api：(pro), 日志：（logger）
    def __init__(self, engine, pro, logger, ts=None):
        self.engine = engine
        self.pro = pro
        self.logger = logger
        # reload(sys)
        # sys.setdefaultencoding('utf-8')

    """
     港股通十大成交股
     接口：ggt_top10
     描述：获取港股通每日成交数据，其中包括沪市、深市详细数据
        ts_code	str	N	股票代码（二选一）
        trade_date	str	N	交易日期（二选一）
        start_date	str	N	开始日期
        end_date	str	N	结束日期
        market_type	str	N	市场类型 2：港股通（沪） 4：港股通（深）

     """

    def ggt_top10(self, ts_code=None, trade_date=None, start_date=None, end_date=None, market_type=None):
        full_name = "TuShare 市场参考数据 港股通十大成交股 ggt_top10"
        parameter = str(
            {'ts_code': strUtils.noneToUndecided(ts_code), 'trade_date': strUtils.noneToUndecided(trade_date),
             'start_date': strUtils.noneToUndecided(start_date), 'end_date': strUtils.noneToUndecided(end_date)})
        try:
            data = self.pro.ggt_top10(ts_code=ts_code, trade_date=trade_date, start_date=start_date,
                                      end_date=end_date)
            data.to_sql("reference_ggt_top10", self.engine, if_exists="append", index=False)
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="ggt_top10",
                                  parameter=parameter,
                                  status=1, result_count=str(len(data)))
        except Exception as e:
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="ggt_top10",
                                  parameter=parameter,
                                  status=0, error_info=str(e), result_count=None)

    """
     融资融券交易汇总
     接口：margin
     描述：获取融资融券每日交易汇总数据
        trade_date	str	N	交易日期
        exchange_id	str	N	交易所代码
        start_date	str	N	开始日期
        end_date	str	N	结束日期
     """

    def margin(self, trade_date=None, exchange_id=None, start_date=None, end_date=None):
        full_name = "TuShare 市场参考数据 融资融券交易汇总 margin"
        parameter = str(
            {'trade_date': strUtils.noneToUndecided(trade_date), 'exchange_id': strUtils.noneToUndecided(exchange_id),
             'start_date': strUtils.noneToUndecided(start_date), 'end_date': strUtils.noneToUndecided(end_date)})
        try:
            data = self.pro.margin(trade_date=trade_date, exchange_id=exchange_id, start_date=start_date,
                                   end_date=end_date)
            data.to_sql("reference_margin", self.engine, if_exists="append", index=False)
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="margin",
                                  parameter=parameter,
                                  status=1, result_count=str(len(data)))
        except Exception as e:
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="margin",
                                  parameter=parameter,
                                  status=0, error_info=str(e), result_count=None)

    """
     融资融券交易明细
     接口：margin
     描述：获取沪深两市每日融资融券明细
        trade_date	str	N	交易日期
        ts_code	str	N	TS代码
        start_date	str	N	开始日期
        end_date	str	N	结束日期
     """

    def margin_detail(self, ts_code=None, trade_date=None, start_date=None, end_date=None):
        full_name = "TuShare 市场参考数据 融资融券交易明细 margin_detail"
        parameter = str(
            {'ts_code': strUtils.noneToUndecided(ts_code), 'trade_date': strUtils.noneToUndecided(trade_date),
             'start_date': strUtils.noneToUndecided(start_date), 'end_date': strUtils.noneToUndecided(end_date)})
        try:
            data = self.pro.margin_detail(ts_code=ts_code, trade_date=trade_date, start_date=start_date,
                                          end_date=end_date)
            data.to_sql("reference_margin_detail", self.engine, if_exists="append", index=False)
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="margin_detail",
                                  parameter=parameter,
                                  status=1, result_count=str(len(data)))
        except Exception as e:
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="margin_detail",
                                  parameter=parameter,
                                  status=0, error_info=str(e), result_count=None)

    """
     前十大股东
     接口：top10_holders
     描述：获取上市公司前十大股东数据，包括持有数量和比例等信息。
        ts_code	str	Y	TS代码
        period	str	N	报告期
        ann_date	str	N	公告日期
        start_date	str	N	报告期开始日期
        end_date	str	N	报告期结束日期
     """

    def top10_holders(self, ts_code=None, period=None, ann_date=None, start_date=None, end_date=None):
        full_name = "TuShare 市场参考数据 前十大股东 top10_holders"
        parameter = str(
            {'ts_code': strUtils.noneToUndecided(ts_code), 'period': strUtils.noneToUndecided(period),
             'ann_date': strUtils.noneToUndecided(ann_date),
             'start_date': strUtils.noneToUndecided(start_date), 'end_date': strUtils.noneToUndecided(end_date)})
        try:
            data = self.pro.top10_holders(ts_code=ts_code, period=period, ann_date=ann_date, start_date=start_date,
                                          end_date=end_date)
            data.to_sql("reference_top10_holders", self.engine, if_exists="append", index=False)
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="top10_holders",
                                  parameter=parameter,
                                  status=1, result_count=str(len(data)))
        except Exception as e:
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="top10_holders",
                                  parameter=parameter,
                                  status=0, error_info=str(e), result_count=None)

    """
     前十大流通股东
     接口：top10_floatholders
     描述：获取上市公司前十大流通股东数据。
        trade_date	str	N	交易日期
        ts_code	str	N	TS代码
        start_date	str	N	开始日期
        end_date	str	N	结束日期
     """

    def top10_floatholders(self, ts_code=None, period=None, ann_date=None, start_date=None, end_date=None):
        full_name = "TuShare 市场参考数据 前十大流通股东 top10_floatholders"
        parameter = str(
            {'ts_code': strUtils.noneToUndecided(ts_code), 'period': strUtils.noneToUndecided(period),
             'ann_date': strUtils.noneToUndecided(ann_date),
             'start_date': strUtils.noneToUndecided(start_date), 'end_date': strUtils.noneToUndecided(end_date)})
        try:
            data = self.pro.top10_floatholders(ts_code=ts_code, period=period, ann_date=ann_date, start_date=start_date,
                                               end_date=end_date)
            data.to_sql("reference_top10_floatholders", self.engine, if_exists="append", index=False)
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="top10_floatholders",
                                  parameter=parameter,
                                  status=1, result_count=str(len(data)))
        except Exception as e:
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="top10_floatholders",
                                  parameter=parameter,
                                  status=0, error_info=str(e), result_count=None)

    """
     龙虎榜每日明细
     接口：top_list
     描述：龙虎榜每日交易明细。
        trade_date	str	Y	交易日期
        ts_code	str	N	股票代码
     """

    def top_list(self, ts_code=None, trade_date=None):
        full_name = "TuShare 市场参考数据 龙虎榜每日明细 top10_floatholders"
        parameter = str(
            {'ts_code': strUtils.noneToUndecided(ts_code), 'trade_date': strUtils.noneToUndecided(trade_date)})
        try:
            data = self.pro.top_list(ts_code=ts_code, trade_date=trade_date)
            data.to_sql("reference_top_list", self.engine, if_exists="append", index=False)
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="top_list",
                                  parameter=parameter,
                                  status=1, result_count=str(len(data)))
        except Exception as e:
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="top_list",
                                  parameter=parameter,
                                  status=0, error_info=str(e), result_count=None)

    """
     龙虎榜机构明细
     接口：top_inst
     描述：龙虎榜机构成交明细。
        trade_date	str	Y	交易日期
        ts_code	str	N	股票代码
     """

    def top_inst(self, ts_code=None, trade_date=None):
        full_name = "TuShare 市场参考数据 龙虎榜机构明细 top10_floatholders"
        parameter = str(
            {'ts_code': strUtils.noneToUndecided(ts_code), 'trade_date': strUtils.noneToUndecided(trade_date)})
        try:
            data = self.pro.top_inst(ts_code=ts_code, trade_date=trade_date)
            data.to_sql("reference_top_inst", self.engine, if_exists="append", index=False)
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="top_inst",
                                  parameter=parameter,
                                  status=1, result_count=str(len(data)))
        except Exception as e:
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="top_inst",
                                  parameter=parameter,
                                  status=0, error_info=str(e), result_count=None)

    """
     股权质押统计数据
     接口：pledge_stat
     描述：获取股票质押统计数据。
        trade_date	str	Y	交易日期
        ts_code	str	N	股票代码
     """

    def pledge_stat(self, ts_code=None):
        full_name = "TuShare 市场参考数据 股权质押统计数据 pledge_stat"
        parameter = str(
            {'ts_code': strUtils.noneToUndecided(ts_code)})
        try:
            data = self.pro.pledge_stat(ts_code=ts_code)
            data.to_sql("reference_pledge_stat", self.engine, if_exists="append", index=False)
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="pledge_stat",
                                  parameter=parameter,
                                  status=1, result_count=str(len(data)))
        except Exception as e:
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="pledge_stat",
                                  parameter=parameter,
                                  status=0, error_info=str(e), result_count=None)

    """
     股权质押明细
     接口：pledge_detail
     描述：获取股票质押明细数据。
        trade_date	str	Y	交易日期
        ts_code	str	N	股票代码
     """

    def pledge_detail(self, ts_code=None):
        full_name = "TuShare 市场参考数据 股权质押明细 pledge_detail"
        parameter = str(
            {'ts_code': strUtils.noneToUndecided(ts_code)})
        try:
            data = self.pro.pledge_detail(ts_code=ts_code)
            data.to_sql("reference_pledge_detail", self.engine, if_exists="append", index=False)
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="pledge_detail",
                                  parameter=parameter,
                                  status=1, result_count=str(len(data)))
        except Exception as e:
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="pledge_detail",
                                  parameter=parameter,
                                  status=0, error_info=str(e), result_count=None)

    """
     股票回购
     接口：repurchase
     描述：获取股票质押明细数据。
        ann_date	str	N	公告日期（任意填参数，如果都不填，单次默认返回2000条）
        start_date	str	N	公告开始日期
        end_date	str	N	公告结束日期
     """

    def repurchase(self, ann_date=None, start_date=None, end_date=None):
        full_name = "TuShare 市场参考数据 股票回购 pledge_detail"
        parameter = str(
            {'ann_date': strUtils.noneToUndecided(ann_date), 'start_date': strUtils.noneToUndecided(start_date),
             'end_date': strUtils.noneToUndecided(end_date)})
        try:
            data = self.pro.repurchase(ann_date=ann_date, start_date=start_date, end_date=end_date)
            data.to_sql("reference_repurchase", self.engine, if_exists="append", index=False)
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="repurchase",
                                  parameter=parameter,
                                  status=1, result_count=str(len(data)))
        except Exception as e:
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="repurchase",
                                  parameter=parameter,
                                  status=0, error_info=str(e), result_count=None)

    """
     概念股分类
     接口：concept
     描述：获取概念股分类，目前只有ts一个来源，未来将逐步增加来源。
        src	str	N	来源，默认为ts
     """

    def concept(self, src=None):
        full_name = "TuShare 市场参考数据 概念股分类 concept"
        parameter = str(
            {'src': strUtils.noneToUndecided(src)})
        try:
            try:
                self.engine.execute("TRUNCATE reference_concept")
            except Exception as e:
                self.logger.info(full_name + str(e))
            data = self.pro.concept(src=src)
            data.to_sql("reference_concept", self.engine, if_exists="append", index=False)
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="concept",
                                  parameter=parameter,
                                  status=1, result_count=str(len(data)))
        except Exception as e:
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="concept",
                                  parameter=parameter,
                                  status=0, error_info=str(e), result_count=None)

    """
     概念股列表
     接口：concept_detail
     描述：获取概念股分类，目前只有ts一个来源，未来将逐步增加来源。
        id	str	N	概念分类ID （id来自概念股分类接口）
        ts_code	str	N	股票代码 （以上参数二选一）
     """

    def concept_detail(self, id=None, ts_code=None):
        full_name = "TuShare 市场参考数据 概念股列表 concept_detail"

        try:
            # data_concept =self.engine.executeQuery("select code from reference_concept")
            sql = "select code from reference_concept"
            concept = pd.read_sql(sql, self.engine)
            # if not pd.isnull(concept).date[0]:
            for index, row in concept.iterrows():
                time.sleep(3)
                parameter = str({'id': strUtils.noneToUndecided(row.code), 'ts_code': strUtils.noneToUndecided(ts_code)})
                try:
                    data = self.pro.concept_detail(id=row.code, ts_code=ts_code)
                    data.to_sql("reference_concept_detail", self.engine, if_exists="append", index=False)
                    # self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="concept_detail",
                    #                       parameter=parameter,
                    #                       status=1, result_count=str(len(data)))
                except Exception as e:
                    self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="concept_detail",
                                          parameter=parameter,
                                          status=0, error_info=str(e), result_count=None)
        except Exception as e:
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="concept_detail",
                                  status=0, error_info=str(e), result_count=None)

    """
      限售股解禁
      接口：share_float
      描述：获取限售股解禁。
        ts_code	str	N	TS股票代码（至少输入一个参数）
        ann_date	str	N	公告日期（日期格式：YYYYMMDD，下同）
        float_date	str	N	解禁日期
        start_date	str	N	解禁开始日期
        end_date	str	N	解禁结束日期
      """

    def share_float(self, ts_code=None, ann_date=None, float_date=None, start_date=None, end_date=None):
        full_name = "TuShare 市场参考数据 股票回购 pledge_detail"
        parameter = str(
            {'ts_code': strUtils.noneToUndecided(ts_code), 'ann_date': strUtils.noneToUndecided(ann_date),
             'start_date': strUtils.noneToUndecided(start_date),
             'float_date': strUtils.noneToUndecided(float_date), 'end_date': strUtils.noneToUndecided(end_date)})
        try:
            data = self.pro.share_float(ts_code=ts_code, ann_date=ann_date, float_date=float_date,
                                        start_date=start_date, end_date=end_date)
            data.to_sql("reference_share_float", self.engine, if_exists="append", index=False)
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="share_float",
                                  parameter=parameter,
                                  status=1, result_count=str(len(data)))
        except Exception as e:
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="share_float",
                                  parameter=parameter,
                                  status=0, error_info=str(e), result_count=None)

    """
      大宗交易
      接口：block_trade
      描述：大宗交易。
        ts_code	str	N	TS代码（股票代码和日期至少输入一个参数）
        trade_date	str	N	交易日期（格式：YYYYMMDD，下同）
        start_date	str	N	开始日期
        end_date	str	N	结束日期
      """

    def block_trade(self, ts_code=None, trade_date=None, start_date=None, end_date=None):
        full_name = "TuShare 市场参考数据 大宗交易 block_trade"
        parameter = str(
            {'ts_code': strUtils.noneToUndecided(ts_code), 'trade_date': strUtils.noneToUndecided(trade_date),
             'start_date': strUtils.noneToUndecided(start_date), 'end_date': strUtils.noneToUndecided(end_date)})
        try:
            data = self.pro.block_trade(ts_code=ts_code, trade_date=trade_date,
                                        start_date=start_date, end_date=end_date)
            data.to_sql("reference_block_trade", self.engine, if_exists="append", index=False)
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="block_trade",
                                  parameter=parameter,
                                  status=1, result_count=str(len(data)))
        except Exception as e:
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="block_trade",
                                  parameter=parameter,
                                  status=0, error_info=str(e), result_count=None)


    """
      股票账户开户数据
      接口：stk_account
      描述：获取股票账户开户数据，统计周期为一周。
        date	str	N	日期
        start_date	str	N	开始日期
        end_date	str	N	结束日期
      """

    def stk_account(self, date=None, start_date=None, end_date=None):
        full_name = "TuShare 市场参考数据 大宗交易 stk_account"
        parameter = str(
            {'date': strUtils.noneToUndecided(date),
             'start_date': strUtils.noneToUndecided(start_date), 'end_date': strUtils.noneToUndecided(end_date)})
        try:
            data = self.pro.stk_account( date=date,
                                        start_date=start_date, end_date=end_date)
            data.to_sql("reference_stk_account", self.engine, if_exists="append", index=False)
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="stk_account",
                                  parameter=parameter,
                                  status=1, result_count=str(len(data)))
        except Exception as e:
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="stk_account",
                                  parameter=parameter,
                                  status=0, error_info=str(e), result_count=None)


    """
         股东增减持
         接口：stk_holdertrade
         更新时间：交易日每天15点～16点之间
         描述：获取上市公司增减持数据，了解重要股东近期及历史上的股份增减变化
            ts_code	str	N	TS股票代码
            ann_date	str	N	公告日期
            start_date	str	N	公告开始日期
            end_date	str	N	公告结束日期
            trade_type	str	N	交易类型IN增持DE减持
            holder_type	str	N	股东类型C公司P个人G高管
         """


    def stk_holdertrade(self, ts_code=None, ann_date=None, start_date=None, end_date=None):
        full_name = "TuShare 市场参考数据 股东增减持 stk_holdertrade"
        parameter = str(
            {'ts_code': strUtils.noneToUndecided(ts_code), 'ann_date': strUtils.noneToUndecided(ann_date),
             'start_date': strUtils.noneToUndecided(start_date), 'end_date': strUtils.noneToUndecided(end_date)})
        try:
            data = self.pro.stk_holdertrade(ts_code=ts_code, ann_date=ann_date, start_date=start_date,
                                            end_date=end_date)
            data.to_sql("reference_stk_holdertrade", self.engine, if_exists="append", index=False)
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="stk_holdertrade",
                                  parameter=parameter,
                                  status=1, result_count=str(len(data)))
        except Exception as e:
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="stk_holdertrade",
                                  parameter=parameter,
                                  status=0, error_info=str(e), result_count=None)

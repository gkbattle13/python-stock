#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd

from tushare_data.utils import strUtils


# 财务数据  利润表， 负载表，现金流量表，业绩预告，业绩快报，分红送股，财务指标数据，财务审计意见，主营业务构成，财报披露日期
class financial_statements():

    # 初始化: 数据连接：(engine), tushare api：(pro), 日志：（logger）
    def __init__(self, engine, pro, logger):
        self.engine = engine
        self.pro = pro
        self.logger = logger

    """
    获取所有 数据轮询  单日更新
    """

    def daily_cycle(self, ann_date=None, start_date=None, end_date=None):
        sql = 'select ts_code from basic_stock where list_status = "L" '
        data_1 = pd.read_sql(sql, self.engine)
        for index, row in data_1.iterrows():
            date_2 = row["ts_code"]
            self.financial(ts_code=date_2, ann_date=ann_date, start_date=start_date, end_date=end_date)

    """
    获取所有 数据轮询  季度更新
    """

    def daily_cycle_quarterly(self, period=None, end_date=None):
        sql = 'select ts_code from basic_stock where list_status = "L" '
        data_1 = pd.read_sql(sql, self.engine)
        for index, row in data_1.iterrows():
            date_2 = row["ts_code"]
            self.financial_quarterly(ts_code=date_2, end_date=end_date, period=period)

    """
    获取所有 数据轮询  季度更新
    """

    # 按照季度获取
    def financial_quarterly(self, ts_code=None, end_date=None, period=None):
        try:
            full_name = "TuShare 财务数据 财报披露日期 disclosure_date"
            parameter = str(
                {"ts_code: " + strUtils.noneToUndecided(ts_code) + ", ann_date: " + strUtils.noneToUndecided(end_date)})
            data = self.pro.disclosure_date(ts_code=ts_code, end_date=end_date)
            data.to_sql("financial_disclosure_date", self.engine, if_exists="append", index=False)
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="financial_disclosure_date",
                                  parameter=parameter, status=1, result_count=str(len(data)))

            """ 主营业务构成
            ts_code	str	Y	股票代码
            period	str	N	报告期(每个季度最后一天的日期,比如20171231表示年报)
            type	str	N	类型：P按产品 D按地区（请输入大写字母P或者D）
            start_date	str	N	报告期开始日期
            end_date	str	N	报告期结束日期
            """
            full_name = "TuShare 财务数据 主营业务构成 fina_mainbz"
            parameter = str(
                {"ts_code: " + strUtils.noneToUndecided(ts_code) + ", period: " + strUtils.noneToUndecided(period)})
            data = self.pro.fina_mainbz(ts_code=ts_code, period=period)
            data.to_sql("financial_fina_mainbz", self.engine, if_exists="append", index=False)
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="fina_mainbz",
                                  parameter=parameter, status=1, result_count=str(len(data)))
        except Exception as e:
            self.logger.infoMysql(engine=self.engine, full_name="财务报表季度数据",
                                  fun_name="financial_disclosure_date,fina_mainbz",
                                  status=0, error_info=str(e), result_count=None)

    """
    交易日期
    获取各大交易所交易日历数据, 默认提取的是上交所 2018年10月11号数据限制为1W条， 需要分开获取
        ts_code	str	Y	股票代码
        ann_date	str	N	公告日期
        start_date	str	N	公告开始日期
        end_date	str	N	公告结束日期
        period	str	N	报告期(每个季度最后一天的日期，比如20171231表示年报)
        report_type	str	N	报告类型： 参考下表说明
        comp_type	str	N	公司类型：1一般工商业 2银行 3保险 4证券
    """

    def financial(self, ts_code=None, ann_date=None, start_date=None, end_date=None, period=None, report_type=None,
                  comp_type=None):
        parameter = str("{ts_code: " + strUtils.noneToUndecided(ts_code) + ", ann_date: " + strUtils.noneToUndecided(
            ann_date) + "}")
        try:
            full_name = "TuShare 财务数据 利润表 income"
            parameter = str(
                {"ts_code: " + strUtils.noneToUndecided(ts_code) + ", ann_date: " + strUtils.noneToUndecided(ann_date)
                 + ", start_date: " + strUtils.noneToUndecided(start_date) + ",end_date： " + strUtils.noneToUndecided(
                    end_date)})
            data = self.pro.income(ts_code=ts_code, ann_date=ann_date, start_date=start_date, end_date=end_date,
                                   period=period, report_type=report_type, comp_type=comp_type)
            data.to_sql("financial_income", self.engine, if_exists="append", index=False)
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="income", parameter=parameter,
                                  status=1, result_count=str(len(data)))

            full_name = "TuShare 财务数据 资产负债表 balancesheet"
            data = self.pro.balancesheet(ts_code=ts_code, ann_date=ann_date, start_date=start_date, end_date=end_date,
                                         period=period, report_type=report_type, comp_type=comp_type)
            data.to_sql("financial_balance_sheet", self.engine, if_exists="append", index=False)
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="balancesheet", parameter=parameter,
                                  status=1, result_count=str(len(data)))

            full_name = "TuShare 财务数据 现金流量表 cashflow"
            data = self.pro.cashflow(ts_code=ts_code, ann_date=ann_date, start_date=start_date, end_date=end_date,
                                     period=period, report_type=report_type, comp_type=comp_type)
            data.to_sql("financial_cash_flow", self.engine, if_exists="append", index=False)
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="cashflow", parameter=parameter,
                                  status=1, result_count=str(len(data)))

            """ 业绩预告
            ts_code	str	N	股票代码(二选一)
            ann_date	str	N	公告日期 (二选一)
            start_date	str	N	公告开始日期
            end_date	str	N	公告结束日期
            period	str	N	报告期(每个季度最后一天的日期，比如20171231表示年报)
            type	str	N	预告类型(预增/预减/扭亏/首亏/续亏/续盈/略增/略减)
            """
            full_name = "TuShare 财务数据 业绩预告 forecast"
            parameter = str(
                {"ts_code: " + strUtils.noneToUndecided(ts_code) + ", ann_date: " + strUtils.noneToUndecided(
                    ann_date) + ", start_date: " + strUtils.noneToUndecided(
                    start_date) + ",end_date： " + strUtils.noneToUndecided(end_date)})
            data = self.pro.forecast(ts_code=ts_code, ann_date=ann_date, start_date=start_date, end_date=end_date,
                                     period=period)
            # , fields='ts_code,ann_date,end_date,revenue,operate_profit,total_profit,n_income,total_assets,total_hldr_eqy_exc_min_int,diluted_eps,diluted_roe,yoy_net_profit,bps,yoy_sales,yoy_op,yoy_tp,yoy_dedu_np,yoy_eps,yoy_roe,growth_assets,yoy_equity,growth_bps,or_last_year,op_last_year,tp_last_year,np_last_year,eps_last_year,open_net_assets,open_bps,perf_summary,is_audit,remark')
            data.to_sql("financial_fore_cast", self.engine, if_exists="append", index=False)
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="forecast", parameter=parameter,
                                  status=1, result_count=str(len(data)))

            """  业绩快报
            ts_code	str	N	股票代码(二选一)
            ann_date	str	N	公告日期 (二选一)
            start_date	str	N	公告开始日期
            end_date	str	N	公告结束日期
            period	str	N	报告期(每个季度最后一天的日期，比如20171231表示年报)
            """
            full_name = "TuShare 财务数据 业绩快报 express"
            data = self.pro.express(ts_code=ts_code, ann_date=ann_date, start_date=start_date, end_date=end_date,
                                    period=period)
            data.to_sql("financial_express", self.engine, if_exists="append", index=False)
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="express", parameter=parameter,
                                  status=1, result_count=str(len(data)))

            full_name = "TuShare 财务数据 财务指标数据 fina_indicator"
            data = self.pro.fina_indicator(ts_code=ts_code, ann_date=ann_date, start_date=start_date, end_date=end_date,
                                           period=period)
            data.to_sql("financial_fina_indicator", self.engine, if_exists="append", index=False)
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="fina_indicator",
                                  parameter=parameter, status=1, result_count=str(len(data)))

            full_name = "TuShare 财务数据 财务审计意见 fina_audit"
            data = self.pro.fina_audit(ts_code=ts_code, ann_date=ann_date, start_date=start_date, end_date=end_date,
                                       period=period)
            data.to_sql("financial_fina_audit", self.engine, if_exists="append", index=False)
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="fina_audit",
                                  parameter=parameter, status=1, result_count=str(len(data)))

            """
            ts_code	str	N	TS代码
            ann_date	str	N	公告日
            record_date	str	N	股权登记日期
            ex_date	str	N	除权除息日
            imp_ann_date	str	N	实施公告日
            """
            full_name = "TuShare 财务数据 分红送股 financial_dividend"
            parameter = str(
                {"ts_code: " + strUtils.noneToUndecided(ts_code) + ", ann_date: " + strUtils.noneToUndecided(ann_date)})
            data = self.pro.dividend(ts_code=ts_code, ann_date=ann_date)
            data.to_sql("financial_dividend", self.engine, if_exists="append", index=False)
            self.logger.infoMysql(engine=self.engine, full_name=full_name, fun_name="financial_dividend",
                                  parameter=parameter, status=1, result_count=str(len(data)))
        except Exception as e:
            self.logger.infoMysql(engine=self.engine, full_name="财务报表单日数据", fun_name="daily,basic", parameter=parameter,
                                  status=0, error_info=str(e), result_count=None)

import schedule

import settings

"""
基于schedule实现任务调度
"""
import sys
sys.path.append('./')
from HousingDataStore import data_store
from HousingDataClean.housing_data_clean import HousingMesClean
from HousingDataAnalyse.house_data_analyse import HouseDataAnalyse
import pandas as pd
import datetime
import sqlite3


def housing_data_crawl_job():
    data_store.data_store_sqlite(str(datetime.date.today().strftime("%Y%m%d")), '南京')
    print("数据爬取结束时间：" + str(datetime.datetime.now()))


def housing_data_clean_job():
    db_path = settings.db_path
    sqlite_conn = sqlite3.connect(db_path)
    yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y%m%d")
    table_name = "lian_jia_data_{time}_tb".format(time=yesterday)
    read_sql = "select * from {table_name}".format(table_name=table_name)
    df = pd.read_sql(read_sql, sqlite_conn)
    housing_mes_clean = HousingMesClean(data_time=yesterday, housing_data=df)
    pd.set_option('display.max_rows', 10)  # 打印最大行数
    pd.set_option('display.max_columns', 10)  # 打印最大列数
    df_handle: pd.DataFrame = housing_mes_clean.date_handle()
    print(df_handle)
    df_filter: pd.DataFrame = housing_mes_clean.data_filter()
    print(df_filter)
    sqlite_conn.close()


def housing_data_analyse_job():
    db_path = settings.db_path
    sqlite_conn = sqlite3.connect(db_path)
    yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y%m%d")
    handle_table_name = "lian_jia_handle_data_{a}_tb".format(a=yesterday)
    filter_table_name = "lian_jia_filter_data_{a}_tb".format(a=yesterday)
    handle_read_sql = "select * from {table_name}".format(table_name=handle_table_name)
    filter_read_sql = "select * from {table_name}".format(table_name=filter_table_name)
    df01 = pd.read_sql(handle_read_sql, sqlite_conn)
    df02 = pd.read_sql(filter_read_sql, sqlite_conn)
    house_data_analyse = HouseDataAnalyse(global_df=df01, target_df=df02, data_date=yesterday)
    house_data_analyse.create_report()


if __name__ == '__main__':
    schedule.every().day.at("20:30").do(housing_data_crawl_job())
    schedule.every().day.at("2:30").do(housing_data_clean_job())
    schedule.every().day.at("4:30").do(housing_data_analyse_job())

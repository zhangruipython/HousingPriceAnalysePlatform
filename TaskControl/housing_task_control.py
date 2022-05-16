import schedule

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
import settings

yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y%m%d")


def housing_data_crawl_job():
    data_store.data_store_sqlite(str(datetime.date.today().strftime("%Y%m%d")), '南京')
    print("数据爬取结束时间：" + str(datetime.datetime.now()))


def housing_data_clean_job(data_date=yesterday):
    db_path = settings.db_path
    sqlite_conn = sqlite3.connect(db_path)

    table_name = "lian_jia_data_{time}_tb".format(time=data_date)
    read_sql = "select * from {table_name}".format(table_name=table_name)
    df = pd.read_sql(read_sql, sqlite_conn)
    housing_mes_clean = HousingMesClean(data_time=data_date, housing_data=df)
    pd.set_option('display.max_rows', 10)  # 打印最大行数
    pd.set_option('display.max_columns', 10)  # 打印最大列数
    df_handle: pd.DataFrame = housing_mes_clean.date_handle()
    print(df_handle["housing_price"])
    df_filter: pd.DataFrame = housing_mes_clean.data_filter()
    print(df_filter["housing_price"])
    sqlite_conn.close()


def housing_data_analyse_job(data_date=yesterday):
    db_path = settings.db_path
    sqlite_conn = sqlite3.connect(db_path)
    handle_table_name = "lian_jia_handle_data_{a}_tb".format(a=data_date)
    filter_table_name = "lian_jia_filter_data_{a}_tb".format(a=data_date)
    handle_read_sql = "select * from {table_name}".format(table_name=handle_table_name)
    filter_read_sql = "select * from {table_name}".format(table_name=filter_table_name)
    df01 = pd.read_sql(handle_read_sql, sqlite_conn)
    df02 = pd.read_sql(filter_read_sql, sqlite_conn)
    house_data_analyse = HouseDataAnalyse(global_df=df01, target_df=df02, data_date=data_date)
    house_data_analyse.create_report()


if __name__ == '__main__':
    # housing_data_clean_job(str(datetime.date.today().strftime("%Y%m%d")))
    # housing_data_analyse_job(str(datetime.date.today().strftime("%Y%m%d")))
    schedule.every().day.at("21:30").do(housing_data_crawl_job())
    schedule.every().day.at("6:30").do(housing_data_clean_job())
    schedule.every().day.at("7:30").do(housing_data_analyse_job())

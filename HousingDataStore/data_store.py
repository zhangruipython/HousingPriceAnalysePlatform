"""
@Time    : 2022-04-09 10:38
@Author  : zhangrui
@FileName: data_store.py
@Software: PyCharm
爬取数据存储
"""
import csv
import sqlite3
from datetime import date
from HousingDataCrawl.housing_mes_spider import HousingPriceSpider


def data_store_csv(spider_data_date, spider_city_name, csv_path, csv_type="英文"):
    csv_header_en = ["data_time", "city_name", "city_region",
                     "housing_estate", "housing_publish_date",
                     "before_days", "housing_follower",
                     "business_area", "housing_type", "housing_area",
                     "housing_orientation",
                     "housing_decoration", "housing_floor", "housing_build_year", "housing_build_mes",
                     "housing_price",
                     "housing_unit_price", "housing_intro_url", "intro", "elevator_housing_ratio",
                     "housing_mes_type",
                     "if_elevator"]
    csv_header_zh = ["数据收集日期", "城市名称", "所属区域", "小区名称", "房源信息发布日期", "房源已经发布天数", "房源当前关注人数", "小区商圈",
                     "户型", "面积", "朝向", "装修情况", "楼层情况", "建筑年份",
                     "建筑结构", "房屋总价信息", "房屋单价", "房屋具体信息网页链接", "房屋卖点", "梯户比例", "房屋属性(商品房还是住宅房)", "是否有电梯"]
    if csv_type == "英文":
        csv_header = csv_header_en
    else:
        csv_header = csv_header_zh
    housing_price_spider = HousingPriceSpider(spider_data_date, spider_city_name)
    with open(csv_path, 'a+', encoding='utf-8') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(csv_header)
        for line in housing_price_spider.start_crawl():
            csv_writer.writerow(line)


def data_store_sqlite(spider_data_date, spider_city_name, db_name="housing_data_db"):
    """
    按天分表
    :param spider_data_date:
    :param spider_city_name:
    :param db_name:
    :return:
    """
    row_num = 0
    row_list = []
    sqlite_conn = sqlite3.connect(db_name)
    table_name = "lian_jia_data_{time}_tb".format(time=spider_data_date)
    cur = sqlite_conn.cursor()
    create_table_sql = "CREATE TABLE IF NOT EXISTS {table_name} (data_time text, city_name text, city_region text," \
                       "housing_estate text, housing_publish_date text,before_days text, housing_follower text," \
                       "business_area text, housing_type text,housing_area text,housing_orientation text," \
                       "housing_decoration text, housing_floor text, housing_build_year text, housing_build_mes text," \
                       "housing_price text,housing_unit_price text, housing_intro_url text, intro text, " \
                       "elevator_housing_ratio text,housing_mes_type text,if_elevator text)" \
        .format(table_name=table_name)
    try:
        cur.execute(create_table_sql)
        sqlite_conn.commit()
    except ConnectionError as e:
        sqlite_conn.rollback()

    # 数据插入
    insert_sql = """INSERT INTO {table_name} (data_time,city_name,city_region,housing_estate,housing_publish_date, 
                 before_days,housing_follower,business_area,housing_type,housing_area,housing_orientation, \
                 housing_decoration,housing_floor,housing_build_year,housing_build_mes,housing_price, \
                 housing_unit_price,housing_intro_url,intro,elevator_housing_ratio,housing_mes_type,if_elevator) \
                 values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);""".format(table_name=table_name)

    housing_price_spider = HousingPriceSpider(spider_data_date, spider_city_name)

    for line in housing_price_spider.start_crawl():
        row_list.append(tuple(line))
        row_num += 1
        if row_num == 500:
            try:
                cur.executemany(insert_sql, row_list)
                sqlite_conn.commit()
                row_num = 0
                row_list.clear()
            except ConnectionError as e:
                sqlite_conn.rollback()
                sqlite_conn.close()
                break
    cur.executemany(insert_sql, row_list)
    sqlite_conn.commit()
    sqlite_conn.close()


if __name__ == '__main__':
    data_store_csv(str(date.today()), '南京', str(date.today()) + '链家数据.csv')
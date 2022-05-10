"""
@Time    : 2022-04-14 10:38
@Author  : zhangrui
@FileName: housing_data_clean.py
@Software: PyCharm
链家网二手房数据清洗
"""
import pandas as pd
import sqlite3
import settings


class HousingMesClean:
    def __init__(self, data_time, housing_data: pd.DataFrame):
        """
        :param housing_data: 链家二手房数据
        """
        self.data_time = data_time
        self.housing_data = housing_data
        self.housing_price_limit = settings.housing_price_limit
        self.housing_area_max_limit = settings.housing_area_max_limit
        self.housing_area_min_limit = settings.housing_area_min_limit
        self.housing_region_limit = settings.housing_region_limit
        self.housing_building_year_limit = settings.housing_building_year_limit
        self.housing_floor_limit = settings.housing_floor_limit
        self.if_elevator_limit = settings.if_elevator_limit
        self.db_path = settings.db_path

    def date_handle(self):
        """
        数据处理
        1、将housing_unit_price转为int类型
        2、将intro转为double类型
        3、建筑年份(housing_build_year)转为int类型
        4、楼层（housing_floor）提取数字
        5、有电梯为true
        :return:
        """
        housing_data_drop_duplicates = self.housing_data.drop_duplicates(ignore_index=True)
        housing_data_handle = housing_data_drop_duplicates.query('housing_build_year.str.contains("年") & '
                                                                 'housing_unit_price!="没有该信息"')
        housing_data_handle['housing_unit_price'] = housing_data_handle.housing_unit_price \
            .str.replace(',', '').str.replace('元/平', '').astype('int32')

        housing_data_handle['housing_area'] = housing_data_handle.housing_area.str.replace('平米', '').astype('double')

        housing_data_handle['housing_build_year'] = housing_data_handle.housing_build_year.str.replace('年建', '') \
            .astype('int32')

        housing_data_handle['housing_floor'] = housing_data_handle.housing_floor.str.extract('(\\d+)').astype(
            'int32')
        housing_data_handle['housing_price'] = housing_data_handle['housing_price'].astype('float64')
        # 将结果写入sqlite数据库
        sqlite_conn = sqlite3.connect(self.db_path)
        table_name = "lian_jia_handle_data_{time}_tb".format(time=self.data_time)
        try:
            housing_data_handle.to_sql(table_name, sqlite_conn, if_exists="replace")
            print(f"将结果写入{table_name}")
            sqlite_conn.close()
        except ConnectionError as e:
            print(e)
            sqlite_conn.rollback()
            sqlite_conn.close()
        return housing_data_handle

    def data_filter(self):
        """
        1、数据进行条件筛选
        2、将目标数据生成唯一ID
        :return: dataframe
        """

        handle_df = self.date_handle()
        filter_condition = 'housing_area<{housing_area_max_limit} & housing_area>{housing_area_min_limit} ' \
                           '& housing_unit_price<{housing_price_limit}  ' \
                           '& housing_build_year>{housing_building_year_limit} ' \
                           '& housing_floor <= {housing_floor_limit} & if_elevator == {if_elevator_limit}' \
                           '& city_region in {housing_region_limit}' \
            .format(housing_area_max_limit=self.housing_area_max_limit,
                    housing_area_min_limit=self.housing_area_min_limit, housing_price_limit=self.housing_price_limit,
                    housing_building_year_limit=self.housing_building_year_limit,
                    housing_floor_limit=self.housing_floor_limit, if_elevator_limit=self.if_elevator_limit,
                    housing_region_limit=self.housing_region_limit)
        filter_df = handle_df.query(filter_condition)
        # 通过MD5算法生成唯一ID

        # 将结果写入sqlite数据库
        sqlite_conn = sqlite3.connect(self.db_path)
        table_name = "lian_jia_filter_data_{time}_tb".format(time=self.data_time)
        try:
            filter_df.to_sql(table_name, sqlite_conn, if_exists="replace")
            print(f"将结果写入{table_name}")
            sqlite_conn.close()
        except ConnectionError as e:
            print(e)
            sqlite_conn.rollback()
            sqlite_conn.close()
        return filter_df


if __name__ == '__main__':
    df = pd.read_csv("../2022-05-01链家数据.csv")
    pd.set_option('display.max_columns', 1000)
    housingMesClean = HousingMesClean("2022-05-01", housing_data=df)
    df_handle = housingMesClean.date_handle()
    df_handle.to_csv("../csv_store/2022-05-01处理过链家数据.csv")
    df_filter = housingMesClean.data_filter()
    df_filter.to_csv("../csv_store/2022-05-01目标链家数据.csv")

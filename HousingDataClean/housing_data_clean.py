"""
@Time    : 2022-04-14 10:38
@Author  : zhangrui
@FileName: housing_data_clean.py
@Software: PyCharm
链家网二手房数据清洗
"""
import pandas as pd
import sqlite3
from datetime import date


class HousingMesClean:
    def __init__(self, housing_data: pd.DataFrame):
        """
        :param housing_data: 链家二手房数据
        """
        self.housing_data = housing_data
        self.housing_price_limit = 30000  # 单价低于3万元
        self.housing_area_limit = 110  # 面积低于110平米
        self.housing_region_limit = ['鼓楼', '建邺', '秦淮', '玄武', '雨花台', '栖霞', '江宁']  # 区域限制
        self.housing_building_year_limit = 2005  # 建筑年份为05年之后
        self.housing_floor_limit = 18  # 楼层高度低于18层
        self.if_elevator_limit = '"有"'  # 有电梯
        self.db_path = "D:/MyProject/HousingPriceAnalysePlatform/sqlite_db/housing_data_db"

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

        housing_data_handle['housing_floor'] = housing_data_handle.housing_floor.str.extract('(\d+)').astype(
            'int32')
        return housing_data_handle

    def data_filter(self):
        """
        1、数据进行条件筛选
        2、将目标数据生成唯一ID
        :return: dataframe
        """

        handle_df = self.date_handle()
        filter_condition = 'housing_area<{housing_area_limit} & housing_unit_price<{housing_price_limit}  ' \
                           '& housing_build_year>{housing_building_year_limit} ' \
                           '& housing_floor <= {housing_floor_limit} & if_elevator == {if_elevator_limit}' \
            .format(housing_area_limit=self.housing_area_limit, housing_price_limit=self.housing_price_limit,
                    housing_building_year_limit=self.housing_building_year_limit,
                    housing_floor_limit=self.housing_floor_limit, if_elevator_limit=self.if_elevator_limit)
        filter_df = handle_df.query(filter_condition)
        # 通过MD5算法生成唯一ID

        # 将结果写入sqlite数据库
        sqlite_conn = sqlite3.connect(self.db_path)
        table_name = "lian_jia_filter_data_{time}_tb".format(time=str(date.today().strftime("%Y%m%d")))
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
    df = pd.read_csv("../HousingDataStore/2022-04-26链家数据.csv")
    pd.set_option('display.max_columns', 1000)
    housingMesClean = HousingMesClean(housing_data=df)
    df_handle = housingMesClean.date_handle()
    df_handle.to_csv("../HousingDataStore/2022-04-26处理过链家数据.csv")
    df_handle = housingMesClean.data_filter()
    df_handle.to_csv("../HousingDataStore/2022-04-26目标链家数据.csv")



"""
@Time    : 2022-04-014 10:38
@Author  : zhangrui
@FileName: housing_mes_analyse.py
@Software: PyCharm
分析链家网二手房数据
"""
import pandas as pd


class HousingMesAnalyse:
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
        self.if_elevator_limit = True  # 有电梯

    def date_handle(self):
        """
        数据处理
        1、将housing_unit_price转为int类型
        2、将intro转为double类型
        3、建筑年份(housing_build_year)转为int类型
        4、楼层（housing_floor）提取数字
        :return:
        """
        housing_data_handle = self.housing_data.query('housing_build_year.str.contains("年")')
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
        数据条件筛选
        :return: dataframe
        """
        handle_df = self.date_handle()
        filter_df = handle_df.query('housing_area<110 & housing_unit_price<30000  & housing_build_year>2005 & '
                                    'housing_floor <= 18 & if_elevator == "有"')
        return filter_df


if __name__ == '__main__':
    df = pd.read_csv("../HousingDataCrawl/2022-04-25链家数据.csv")
    pd.set_option('display.max_columns', 1000)
    housingMesAnalyse = HousingMesAnalyse(df)
    df_handle = housingMesAnalyse.data_filter()
    df_handle.to_csv("2022-04-25处理过链家数据.csv")

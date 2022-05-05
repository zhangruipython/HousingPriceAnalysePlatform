import pandas as pd
import sqlite3
from pyecharts.charts import Bar, Line, Tab, Grid, Page, Pie
import pyecharts.options as opts
from pyecharts.components import Table
from snapshot_phantomjs import snapshot
from pyecharts.globals import ThemeType
from pyecharts.render import make_snapshot


class HouseDataAnalyse:
    def __init__(self, global_df: pd.DataFrame, target_df: pd.DataFrame, data_date):
        """
        数据分析与可视化
        :param global_df: 全量清洗数据
        :param target_df: 目标数据
        :param data_date: 数据日期
        """
        self.global_df = global_df
        self.target_df = target_df
        self.data_date = data_date
        self.db_path = "D:/MyProject/HousingPriceAnalysePlatform/sqlite_db/housing_data_db"
        self.today_clean_table_name = ""
        self.yesterday_clean_table_name = ""

    def global_data_analyse(self):
        """
        全量数据分析
        1、计算城市平均房价
        2、计算每个区的房价平均值，方差，平均差
        3、计算每个区在售房源
        :return:
        """
        # 区域名称
        region_name_list = []
        # 区域二手房总数
        region_housing_num_list = []
        # 区域房价最高值
        region_housing_max_list = []
        # 区域房价最低值
        region_housing_min_list = []
        # 区域房价平均值
        region_housing_avg_list = []
        # 区域房价中位数
        region_housing_median_list = []
        # 区域房价标准差
        region_housing_std_list = []
        # 区域房价方差
        region_housing_var_list = []

        city_mean_price = round(self.global_df['housing_unit_price'].mean(), 2)
        all_list = []
        groups = self.global_df.groupby("city_region")
        for group_name, group_df in groups:
            f_se = group_df['housing_unit_price'].agg(['max', 'min', 'mean', 'median', 'std', 'var'])
            f_count = group_df['city_region'].agg("count")
            all_list.append([city_mean_price, group_name, f_count, f_se[0], f_se[1], round(f_se[2], 2), f_se[3]])
            region_name_list.append(group_name)
            region_housing_num_list.append(f_count)
            region_housing_max_list.append(f_se[0])
            region_housing_min_list.append(f_se[1])
            region_housing_avg_list.append(round(f_se[2], 2))
            region_housing_median_list.append(f_se[3])
            region_housing_std_list.append(f_se[4])
            region_housing_var_list.append(f_se[5])

        return all_list, region_name_list, region_housing_num_list, region_housing_max_list, region_housing_min_list, \
               region_housing_avg_list, region_housing_median_list, region_housing_std_list, region_housing_var_list

    def target_df_analyse(self):
        """
        目标数据分析
        1、与昨日相比新增数据
        2、与昨日对比价格不变房源。价格上涨房源，价格下降房源
        2、每个区的每一个符合条件房源价格变化
        :return:
        """
        # 读取sqlite数据库中昨日数据
        read_sql = "select * from {table_name}".format(table_name=self.yesterday_clean_table_name)
        with sqlite3.connect(self.db_path) as conn:
            c = conn.cursor()
            df = pd.read_sql(read_sql, con=c)

    def target_df_visual(self):
        """
        全局数据和目标数据可视化，将图片保存为PDF
        :return:
        """
        all_list, region_name_list, region_housing_num_list, region_housing_max_list, region_housing_min_list, \
        region_housing_avg_list, region_housing_median_list, region_housing_std_list, \
        region_housing_var_list = self.global_data_analyse()
        # 各区域二手房数量
        housing_num_bar = (
            Bar(init_opts=opts.InitOpts(theme=ThemeType.ROMA, )).add_xaxis(region_name_list)
                .add_yaxis("", [str(x) for x in region_housing_num_list]).set_global_opts(
                title_opts=opts.TitleOpts(title="各区域二手房数量"),
                yaxis_opts=opts.AxisOpts(name='数量（个）')
            )
        )
        # 单位面积价格统计
        housing_agg_bar = Line(init_opts=opts.InitOpts(theme=ThemeType.ROMA)).add_xaxis(region_name_list).add_yaxis(
            series_name="单位面积价格最大值",
            y_axis=region_housing_max_list
        ).add_yaxis(
            series_name="单位面积价格最小值",
            y_axis=region_housing_min_list
        ).add_yaxis(
            series_name="单位面积价格平均数",
            y_axis=region_housing_avg_list
        ).set_global_opts(
            title_opts=opts.TitleOpts(title="单位面积价格统计"),
            yaxis_opts=opts.AxisOpts(name='价格（元）')
        )
        # 明细数据表格
        table = Table()
        headers = ["南京二手房平均价格", "区域名称", "二手房数量", "单位面积价格最大值", "单位面积价格最小值", "单位面积价格平均数", "单位面积价格中位数"]

        table.add(headers, all_list).set_global_opts(
            title_opts=opts.ComponentTitleOpts(title="明细数据表格")
        )
        page = Page(layout=Page.SimplePageLayout)
        page.add(housing_num_bar, housing_agg_bar, table)
        page.render()
        make_snapshot(snapshot, housing_num_bar.render(), "{data_date}号全部二手房数据分析.png".format(data_date=self.data_date))


if __name__ == '__main__':
    df01 = pd.read_csv("../HousingDataStore/2022-04-26处理过链家数据.csv")
    df02 = pd.read_csv("../HousingDataStore/2022-04-26目标链家数据.csv")
    houseDataAnalyse = HouseDataAnalyse(global_df=df01, target_df=df02, data_date='2022-04-26')
    houseDataAnalyse.target_df_visual()

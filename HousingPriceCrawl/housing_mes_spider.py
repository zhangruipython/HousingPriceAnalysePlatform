"""
@Time    : 2022-04-09 10:38
@Author  : zhangrui
@FileName: housing_mes_spider.py
@Software: PyCharm
通过链家网爬取二手房数据
"""
from HousingPriceCrawl import city_code
from lxml import etree
from datetime import date
import requests
import datetime
import csv
import time
from fake_useragent import UserAgent


# 简单的反爬，设置一个请求头来伪装成浏览器
def request_header():
    headers = {
        # 'User-Agent': UserAgent().random #常见浏览器的请求头伪装（如：火狐,谷歌）
        'User-Agent': UserAgent().Chrome  # 谷歌浏览器
    }
    return headers


class HousingPriceSpider:
    def __init__(self, data_time, city_name, csv_name):
        """

        :param data_time: 数据日期
        :param city_name: 城市名称
        :param csv_name: 写入CSV文件名称
        """
        self.data_time = data_time
        self.city_name = city_name
        self.csv_name = csv_name
        # self.csv_header = ["数据收集日期", "城市名称", "所属区域", "小区名称", "房源信息发布日期", "房源已经发布天数", "房源当前关注人数", "小区商圈",
        #                    "户型", "面积", "朝向", "装修情况", "楼层情况", "建筑年份",
        #                    "建筑结构", "房屋总价信息", "房屋单价", "房屋具体信息网页链接", "房屋卖点", "梯户比例", "房屋属性(商品房还是住宅房)", "是否有电梯"]

        self.csv_header = ["data_time", "city_name", "city_region",
                           "housing_estate", "housing_publish_date",
                           "before_days", "housing_follower",
                           "business_area", "housing_type", "housing_area",
                           "housing_orientation",
                           "housing_decoration", "housing_floor", "housing_build_year", "housing_build_mes",
                           "housing_price",
                           "housing_unit_price", "housing_intro_url", "intro", "elevator_housing_ratio",
                           "housing_mes_type",
                           "if_elevator"]

    @staticmethod
    def parse_publish_date(publish_mes):
        """
        解析发布信息，获取多少天之前发布的
        :param publish_mes: 发布信息
        :return:
        """
        if publish_mes.__contains__('年'):
            if publish_mes.__contains__('一'):
                return int(365)
            else:
                return int(365 * 2)
        elif publish_mes.__contains__('月'):
            return int(publish_mes.split('/')[1].strip()[0]) * 30
        elif publish_mes.__contains__('天'):
            return int(publish_mes.split('/')[1].strip()[0])

    @staticmethod
    def handle_info(info_url):
        response = requests.get(url=info_url, headers=request_header())
        if response.status_code == 200:
            r = response.text
            s = etree.HTML(r)
            intros = s.xpath('/html/body/div[3]/div/div/div[1]/div/text()')  # 房屋特色介绍
            elevator_housing_ratios = s.xpath(
                '//*[@id="introduction"]/div/div/div[1]/div[2]/ul/li[10]/text()')  # 房屋梯户比例
            housing_types = s.xpath('//*[@id="introduction"]/div/div/div[2]/div[2]/ul/li[2]/span[2]/text()')  # 商品房or住宅房
            if_elevators = s.xpath('//*[@id="introduction"]/div/div/div[1]/div[2]/ul/li[11]/text()')  # 是否有电梯
            if len(intros) > 0:
                intro = intros[0].strip()
            else:
                intro = '没有该信息'
            if len(elevator_housing_ratios) > 0:
                elevator_housing_ratio = elevator_housing_ratios[0].strip()
            else:
                elevator_housing_ratio = '没有该信息'
            if len(housing_types) > 0:
                housing_type = housing_types[0].strip()
            else:
                housing_type = '没有该信息'
            if len(if_elevators) > 0:
                if_elevator = if_elevators[0].strip()
            else:
                if_elevator = '没有该信息'
            return intro, elevator_housing_ratio, housing_type, if_elevator
        else:
            return '没有该信息', '没有该信息', '没有该信息', '没有该信息'

    @staticmethod
    def fill_list(my_list: list, def_length: int, fill='没有该信息'):
        """
        默认以'没有该信息'填充数组至指定长度
        :param my_list: 数组
        :param def_length: 指定长度
        :param fill: 填充对象
        :return: list
        """
        if len(my_list) >= def_length:
            return my_list
        else:
            return my_list + (def_length - len(my_list)) * [fill]

    def start_crawl(self):
        with open(self.csv_name, 'a+', encoding='utf-8') as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow(self.csv_header)
            code = city_code.city_map_code[self.city_name]
            city_regions = city_code.city_regions[self.city_name]
            for city_region in city_regions:
                start_url = 'https://{c_code}.lianjia.com/ershoufang/{region}'.format(
                    c_code=code, region=city_region)
                for i in range(1, 50):
                    time.sleep(20)
                    url = start_url + '/pg' + str(i)
                    response = requests.get(url)
                    if response.status_code == 200:
                        response_result = response.text
                        s = etree.HTML(response_result)
                        # 一页有30套房源
                        for k in range(1, 31):
                            # 小区名称
                            housing_estate_list = s.xpath(
                                '//*[@id="content"]/div[1]/ul/li[{}]/div[1]/div[2]/div/a[1]/text()'.format(k))
                            # 小区商圈
                            business_area_list = s.xpath(
                                '//*[@id="content"]/div[1]/ul/li[{}]/div[1]/div[2]/div/a[2]/text()'.format(k))
                            # 户型总体信息
                            housing_type_list = s.xpath(
                                '//*[@id="content"]/div[1]/ul/li[{}]/div[1]/div[3]/div/text()'.format(k))
                            # 发布信息
                            housing_publish_mes = s.xpath(
                                '//*[@id="content"]/div[1]/ul/li[{}]/div[1]/div[4]/text()'.format(k))
                            # 房屋总价信息
                            housing_price_list = s.xpath(
                                '//*[@id="content"]/div[1]/ul/li[{}]/div[1]/div[6]/div[1]/span/text()'.format(k))
                            # 房屋单价
                            housing_unit_price_list = s.xpath(
                                '//*[@id="content"]/div[1]/ul/li[{}]/div[1]/div[6]/div[2]/span/text()'.format(k))
                            # 房屋卖点（网页链接）
                            housing_intro_urls = s.xpath(
                                '//*[@id="content"]/div[1]/ul/li[{}]/div[1]/div[1]/a/@href'.format(k))

                            if len(housing_estate_list) > 0:
                                housing_estate = housing_estate_list[0].strip()
                            else:
                                housing_estate = '没有该信息'

                            if len(business_area_list) > 0:
                                business_area = business_area_list[0].strip()
                            else:
                                business_area = '没有该信息'

                            if len(housing_type_list) > 0:
                                all_mes = housing_type_list[0].split('|')
                                # 目标数组中有7个元素，如果没有则用'没有该信息'添加
                                all_mes = HousingPriceSpider.fill_list(all_mes, 7)
                                housing_type = all_mes[0].strip()  # 户型
                                housing_area = all_mes[1].strip()  # 面积
                                housing_orientation = all_mes[2].strip()  # 朝向
                                housing_decoration = all_mes[3].strip()  # 装修情况
                                housing_floor = all_mes[4].strip()  # 楼层情况
                                housing_build_year = all_mes[5].strip()  # 建筑年份
                                housing_build_mes = all_mes[6].strip()  # 建筑结构
                            else:
                                housing_type = '没有该信息'
                                housing_area = '没有该信息'
                                housing_orientation = '没有该信息'
                                housing_decoration = '没有该信息'
                                housing_floor = '没有该信息'
                                housing_build_year = '没有该信息'
                                housing_build_mes = '没有该信息'

                            if len(housing_publish_mes) > 0:
                                housing_publish = housing_publish_mes[0].strip()
                                before_days = housingPriceSpider.parse_publish_date(housing_publish)  # 已经发布天数
                                housing_publish_date = date.today() + datetime.timedelta(days=before_days * -1)  # 发布日期
                                housing_follower = housing_publish.split('/')[0].strip()  # 关注人数
                            else:
                                before_days = '没有该信息'
                                housing_publish_date = '没有该信息'
                                housing_follower = '没有该信息'

                            if len(housing_price_list) > 0:
                                housing_price = housing_price_list[0].strip()
                            else:
                                housing_price = '没有该信息'

                            if len(housing_unit_price_list) > 0:
                                housing_unit_price = housing_unit_price_list[0].strip()
                            else:
                                housing_unit_price = '没有该信息'

                            if len(housing_intro_urls) > 0:
                                housing_intro_url = housing_intro_urls[0].strip()
                                # 综合信息
                                intro, elevator_housing_ratio, housing_mes_type, if_elevator = HousingPriceSpider. \
                                    handle_info(housing_intro_url)
                            else:
                                housing_intro_url = '没有该信息'
                                intro = '没有该信息'
                                elevator_housing_ratio = '没有该信息'
                                housing_mes_type = '没有该信息'
                                if_elevator = '没有该信息'
                            # 数据收集日期,城市名称,所属区域,小区名称,房源信息发布日期,房源已经发布天数,房源当前关注人数，小区商圈,户型,面积,朝向,装修情况,楼层情况,建筑年份,
                            # 建筑结构,房屋总价信息,房屋单价,房屋具体信息网页链接,房屋卖点,梯户比例,房屋属性(商品房还是住宅房),是否有电梯,
                            print(date.today(), self.city_name, city_region, housing_estate, housing_publish_date,
                                  before_days, housing_follower,
                                  business_area, housing_type, housing_area,
                                  housing_orientation,
                                  housing_decoration, housing_floor, housing_build_year, housing_build_mes,
                                  housing_price,
                                  housing_unit_price, housing_intro_url, intro, elevator_housing_ratio,
                                  housing_mes_type,
                                  if_elevator)
                            data_row = [date.today(), self.city_name, city_code.city_map_regions[city_region],
                                        housing_estate, housing_publish_date,
                                        before_days, housing_follower,
                                        business_area, housing_type, housing_area,
                                        housing_orientation,
                                        housing_decoration, housing_floor, housing_build_year, housing_build_mes,
                                        housing_price,
                                        housing_unit_price, housing_intro_url, intro, elevator_housing_ratio,
                                        housing_mes_type,
                                        if_elevator]
                            csv_writer.writerow(data_row)


def send_mail():
    pass


if __name__ == '__main__':
    print('my name is {a}'.format(a='zhangrui'))
    housingPriceSpider = HousingPriceSpider(str(date.today()), '南京', str(date.today()) + '链家数据.csv')
    housingPriceSpider.start_crawl()
    # print(housingPriceSpider.handle_info('https://nj.lianjia.com/ershoufang/103119684484.html'))

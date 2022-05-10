city_map_code = {'南京': 'nj'}
# city_regions = {
#     '南京': ['jianye', 'qinhuai', 'xuanwu', 'yuhuatai', 'qixia', 'jiangning', 'pukou', 'liuhe', 'lishui',
#            'gaochun', 'jurong']}
city_regions = {
    '南京': ['jianye', 'qinhuai', 'yuhuatai', 'qixia', 'jiangning', 'pukou', 'liuhe', 'lishui',
           'gaochun', 'jurong', 'xuanwu']}
city_map_regions = {'gulou': '鼓楼', 'jianye': '建邺', 'qinhuai': '秦淮', 'xuanwu': '玄武', 'yuhuatai': '雨花台', 'qixia': '栖霞',
                    'jiangning': '江宁', 'pukou': '浦口', 'liuhe': '六合', 'lishui': '溧水',
                    'gaochun': '高淳', 'jurong': '句容'}
column_dict = {"data_time": "数据收集日期",
               "city_name": "所属区域",
               "city_region": "所属区域",
               "housing_estate": "小区名称",
               "housing_publish_date": "房源信息发布日期",
               "before_days": "房源已经发布天数",
               "housing_follower": "房源当前关注人数",
               "business_area": "小区商圈",
               "housing_type": "户型",
               "housing_area": "面积",
               "housing_orientation": "朝向",
               "housing_decoration": "装修情况",
               "housing_floor": "楼层情况",
               "housing_build_year": "建筑年份",
               "housing_build_mes": "建筑结构",
               "housing_price": "房屋总价信息",
               "housing_unit_price": "房屋单价",
               "housing_intro_url": "房屋具体信息网页链接",
               "intro": "房屋卖点",
               "elevator_housing_ratio": "梯户比例",
               "housing_mes_type": "房屋属性(商品房还是住宅房)",
               "if_elevator": "是否有电梯",
               "model_score": "综合得分"}

# 数据筛选条件
housing_price_limit = 35000  # 单价低于3.5万元
housing_area_max_limit = 110  # 面积低于100平米 大于80平米
housing_area_min_limit = 85  # 面积低于100平米 大于80平米
housing_region_limit = ['鼓楼', '建邺', '秦淮', '玄武', '雨花台', '栖霞', '江宁']  # 区域限制
housing_building_year_limit = 2005  # 建筑年份为05年之后
housing_floor_limit = 18  # 楼层高度低于18层
if_elevator_limit = '"有"'  # 有电梯
# 计算得分权重配置
model_score = {
    "city_region": {"江宁": 80, "雨花台": 90, "栖霞": 90, "鼓楼": 100, "建邺": 100, "秦淮": 100, "玄武": 100},
    "housing_follower": {"0-10": 60, "10-20": 70, "20-30": 80, "30-50": 90, "50-": 100},
    "housing_area": {"80-85": 60, "85-90": 70, "90-95": 80, "95-100": 90, "100-110": 100},
    "housing_build_year": {"2005-2008": 60, "2008-2010": 70, "2010-2015": 80, "2015-2018": 90, "2018-": 100},
    "housing_price": {"320-": 90, "300-320": 95, "-300": 100},
    "metro_intro": {0: 60, 1: 100},
    "housing_mes_type": {"is_commercial_housing": 100, "no_commercial_housing": 60}
}
model_weight = {
    "city_region_score": 0.1,
    "housing_follower_score": 0.05,
    "housing_area_score": 0.25,
    "housing_build_year_score": 0.1,
    "housing_price_score": 0.1,
    "metro_intro_score": 0.2,
    "housing_mes_type_score": 0.2
}
db_path = "D:/MyProject/HousingPriceAnalysePlatform/sqlite_db/housing_data_db"
picture_path = "D:/MyProject/HousingPriceAnalysePlatform/picture_store/"
report_path = "D:/MyProject/HousingPriceAnalysePlatform/report_store/"

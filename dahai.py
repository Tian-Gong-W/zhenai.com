#!/usr/bin/env python3
"""TON MAN 2.0 single-file service.

This file contains the nationwide city crawler, dashboard, Telegram status reporting,
Telegram profile-card pushes, and all persistence logic. Runtime state is kept only in
SQLite; credentials remain in .env.
"""

import csv
import hashlib
import hmac
import io
import json
import os
import queue
import re
import sqlite3
import sys
import tempfile
import threading
import time
import uuid
from datetime import datetime, timedelta
from functools import wraps
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from flask import Flask, Response, jsonify, render_template_string, request, session
from PIL import Image, ImageDraw, ImageFont

BASE_DIR = os.environ.get("ZHENAI_BASE_DIR", os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "zhenai.db")


def load_local_env(env_path):
    """Load .env for direct and systemd execution without overriding environment values."""
    try:
        for raw_line in Path(env_path).read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            name, value = line.split("=", 1)
            name = name.strip()
            value = value.strip().strip("'\"")
            if name and name not in os.environ:
                os.environ[name] = value
    except OSError:
        pass


load_local_env(Path(BASE_DIR) / ".env")
MY_TOKEN = os.environ.get("MY_TOKEN", "")
MY_SEED = os.environ.get("MY_SEED", "")
TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "")
GROUP_CHAT_ID = os.environ.get("GROUP_CHAT_ID", "")
TG_CONTROL_CHAT_ID = os.environ.get("TG_CONTROL_CHAT_ID", "")
TG_CONTROL_USER_ID = os.environ.get("TG_CONTROL_USER_ID", "")
TG_BOT_USERNAME = os.environ.get("TG_BOT_USERNAME", "TopMan_ZiYuan_bot")
TG_PRIVATE_PUSH_ENABLED = os.environ.get("TG_PRIVATE_PUSH_ENABLED", "0") == "1"
SCREENSHOT_TOKEN = os.environ.get("SCREENSHOT_TOKEN", "") or MY_TOKEN
SCREENSHOT_SEED = os.environ.get("SCREENSHOT_SEED", "")
TG_PROFILE_CARD_ENABLED = os.environ.get("TG_PROFILE_CARD_ENABLED", "1") != "0"
DASHBOARD_ACCESS_TOKEN = os.environ.get("DASHBOARD_ACCESS_TOKEN", "")
DASHBOARD_SESSION_SECRET = os.environ.get("DASHBOARD_SESSION_SECRET", "")


def _env_int(name, default):
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def _env_list(name, default):
    raw = os.environ.get(name, "")
    values = [item.strip() for item in raw.split(",") if item.strip()]
    return values or list(default)


DEFAULT_SEARCH_CONFIG = {
    "provinces": _env_list("SEARCH_PROVINCES", ["广东", "山东"]),
    "age_begin": _env_int("SEARCH_AGE_BEGIN", 40),
    "age_end": _env_int("SEARCH_AGE_END", 65),
    "gender": "女",
    "max_pages": _env_int("MAX_CITY_PAGES", 49),
    "minimum_income": 3000,
    "push_window_hours": _env_int("PUSH_WINDOW_HOURS", 48),
    "income_filter": "",
    "education_filter": "",
    "marriage_filter": "",
    "cert_days": 3,
}
EXCLUDED_PROVINCES = {"福建"}
PUSH_INTERVAL_SECONDS = 60
CRAWL_DELAY_SECONDS = 0.5
BEIJING_TZ = ZoneInfo("Asia/Shanghai")
CERTIFICATION_WINDOW_HOURS = 48
AUTO_RESTART_DELAY_SECONDS = _env_int("AUTO_RESTART_DELAY_SECONDS", 30 * 60)
AUTO_RESTART_ENABLED = os.environ.get("AUTO_RESTART_ENABLED", "1") != "0"
SCRAPED_IDS_FILE = os.path.join(BASE_DIR, "scraped_ids.txt")
FONT_CANDIDATES = (
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc",
    "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
)

CITY_CODE_MAP = {
    '朝阳区': 10102005, '海淀区': 10102008, '丰台区': 10102006, '昌平区': 10102013,
    '通州区': 10102011, '大兴区': 10102014, '西城区': 10102002, '房山区': 10102010,
    '顺义区': 10102012, '东城区': 10102001, '石景山区': 10102007, '怀柔区': 10102015,
    '门头沟区': 10102009, '密云区': 10102017, '延庆区': 10102018, '平谷区': 10102016,
    '闸北区': 10103005, '黄浦区': 10103008, '徐汇区': 10103002, '浦东新区': 10103001,
    '闵行区': 10103012, '宝山区': 10103011, '松江区': 10103015, '嘉定区': 10103013,
    '杨浦区': 10103007, '普陀区': 10103004, '青浦区': 10103016, '奉贤区': 10103018,
    '静安区': 10103010, '虹口区': 10103006, '金山区': 10103014, '长宁区': 10103003,
    '崇明区': 10103017,
    '广州': 10101002, '深圳': 10101201, '佛山': 10101003, '湛江': 10101004,
    '珠海': 10101005, '肇庆': 10101006, '东莞': 10101007, '惠州': 10101008,
    '中山': 10101011, '茂名': 10101012, '汕头': 10101013, '梅州': 10101014,
    '韶关': 10101015, '江门': 10101016, '清远': 10101018, '潮州': 10101020,
    '阳江': 10101022, '河源': 10101023, '揭阳': 10101026, '汕尾': 10101028,
    '云浮': 10101068,
    '和平区': 10104008, '河东区': 10104009, '河西区': 10104010, '南开区': 10104011,
    '河北区': 10104012, '红桥区': 10104013, '东丽区': 10104017, '西青区': 10104018,
    '滨海新区': 10104014, '津南区': 10104020, '武清区': 10104002, '宝坻区': 10104005,
    '静海区': 10104006, '宁河区': 10104003, '北辰区': 10104019, '蓟州区': 10104007,
    '渝中区': 10105034, '渝北区': 10105040, '合川区': 10105013, '九龙坡区': 10105037,
    '沙坪坝区': 10105036, '永川区': 10105007, '巴南区': 10105041, '南岸区': 10105038,
    '江北区': 10105026, '北碚区': 10105024, '璧山区': 10105025, '大渡口区': 10105035,
    '万州区': 10105042, '开州区': 10105006, '梁平区': 10105021, '黔江区': 10105018,
    '武隆区': 10105003, '江津区': 10105029, '涪陵区': 10105028, '綦江区': 10105033,
    '大足区': 10105023, '长寿区': 10105016, '荣昌区': 10105008, '铜梁区': 10105010,
    '潼南区': 10105027, '南川区': 10105015, '云阳县': 10105019, '奉节县': 10105002,
    '忠县': 10105004, '垫江县': 10105020, '丰都县': 10105030, '巫山县': 10105005,
    '巫溪县': 10105017, '城口县': 10105032, '石柱土家族自治县': 10105012,
    '酉阳土家族苗族自治县': 10105044, '彭水苗族土家族自治县': 10105045,
    '秀山土家族苗族自治县': 10105043,
    '合肥': 10106001, '淮南': 10106002, '蚌埠': 10106003, '宿州': 10106004,
    '阜阳': 10106005, '六安': 10106006, '滁州': 10106008, '芜湖': 10106009,
    '安庆': 10106011, '黄山': 10106012, '铜陵': 10106013, '淮北': 10106042,
    '亳州': 10106072, '马鞍山': 10106075, '池州': 10106079, '宣城': 10106080,
    '兰州': 10108001, '张掖': 10108002, '武威': 10108003, '酒泉': 10108004,
    '金昌': 10108006, '天水': 10108007, '定西': 10108008, '平凉': 10108009,
    '甘南': 10108012, '嘉峪关': 10108023, '庆阳': 10108064, '白银': 10108078,
    '陇南': 10108084, '临夏': 10108085,
    '南宁': 10109001, '柳州': 10109002, '钦州': 10109003, '百色': 10109004,
    '玉林': 10109005, '防城港': 10109006, '桂林': 10109007, '梧州': 10109008,
    '河池': 10109009, '北海': 10109010, '贵港': 10109014, '来宾': 10109018,
    '崇左': 10109083, '贺州': 10109089,
    '贵阳': 10110001, '六盘水': 10110002, '安顺': 10110006, '遵义': 10110007,
    '毕节': 10110021, '铜仁': 10110055, '黔西南布依族苗族自治州': 10110082,
    '黔东南苗族侗族自治州': 10110083, '黔南布依族苗族自治州': 10110084,
    '海口': 10111001, '三亚': 10111002, '省直辖县级': 10111022,
    '石家庄': 10112001, '衡水': 10112002, '邢台': 10112003, '邯郸': 10112004,
    '沧州': 10112005, '唐山': 10112006, '廊坊': 10112007, '秦皇岛': 10112008,
    '承德': 10112009, '保定': 10112010, '张家口': 10112011,
    '郑州': 10113001, '新乡': 10113002, '安阳': 10113003, '许昌': 10113004,
    '驻马店': 10113005, '漯河': 10113006, '信阳': 10113007, '周口': 10113008,
    '洛阳': 10113009, '平顶山': 10113010, '三门峡': 10113011, '南阳': 10113012,
    '开封': 10113013, '商丘': 10113014, '鹤壁': 10113015, '濮阳': 10113016,
    '焦作': 10113017, '河南省直辖县级': 10113126,
    '武汉': 10115001, '黄石': 10115002, '鄂州': 10115004, '咸宁': 10115006,
    '十堰': 10115007, '宜昌': 10115008, '恩施': 10115009, '荆州': 10115010,
    '黄冈': 10115011, '荆门': 10115012, '孝感': 10115013, '襄阳': 10115014,
    '随州': 10115062, '湖北省直辖县级': 10115085,
    '长沙': 10116001, '株洲': 10116002, '益阳': 10116003, '岳阳': 10116004,
    '常德': 10116005, '娄底': 10116007, '怀化': 10116008, '衡阳': 10116009,
    '邵阳': 10116010, '郴州': 10116011, '张家界': 10116013, '湘潭': 10116014,
    '永州': 10116075, '湘西土家族苗族自治州': 10116097,
    '南京': 10118001, '苏州': 10118002, '无锡': 10118003, '徐州': 10118004,
    '常州': 10118005, '镇江': 10118006, '连云港': 10118007, '盐城': 10118009,
    '扬州': 10118010, '南通': 10118011, '淮安': 10118016, '泰州': 10118051,
    '宿迁': 10118063,
    '南昌': 10119001, '九江': 10119002, '景德镇': 10119003, '上饶': 10119004,
    '鹰潭': 10119005, '宜春': 10119006, '萍乡': 10119007, '赣州': 10119008,
    '吉安': 10119009, '抚州': 10119010, '新余': 10119040,
    '呼和浩特': 10121001, '包头': 10121003, '乌海': 10121005, '赤峰': 10121008,
    '通辽': 10121011, '锡林郭勒盟': 10121089, '阿拉善盟': 10121090, '兴安': 10121091,
    '鄂尔多斯': 10121092, '呼伦贝尔': 10121093, '巴彦淖尔': 10121094, '乌兰察布': 10121095,
    '银川': 10122001, '石嘴山': 10122002, '固原': 10122003, '吴忠': 10122010,
    '中卫': 10122011,
    '西宁': 10123001, '果洛': 10123002, '玉树': 10123003, '海西': 10123005,
    '海东地区': 10123045, '海北藏族自治州': 10123046, '黄南藏族自治州': 10123047,
    '海南藏族自治州': 10123048,
    '青岛': 10124001, '威海': 10124002, '济南': 10124003, '淄博': 10124004,
    '聊城': 10124005, '德州': 10124006, '东营': 10124007, '潍坊': 10124008,
    '烟台': 10124009, '泰安': 10124011, '菏泽': 10124012, '临沂': 10124013,
    '枣庄': 10124014, '济宁': 10124015, '日照': 10124016, '滨州': 10124018,
    '莱芜': 10124068,
    '太原': 10125001, '忻州': 10125003, '大同': 10125005, '临汾': 10125006,
    '运城': 10125008, '阳泉': 10125009, '长治': 10125010, '晋城': 10125011,
    '朔州': 10125034, '晋中': 10125107, '吕梁': 10125108,
    '西安': 10126001, '渭南': 10126002, '延安': 10126003, '榆林': 10126005,
    '宝鸡': 10126006, '安康': 10126007, '汉中': 10126008, '铜川': 10126010,
    '咸阳': 10126011, '商洛': 10126098,
    '成都': 10127001, '乐山': 10127002, '凉山': 10127003, '绵阳': 10127005,
    '阿坝': 10127007, '雅安': 10127008, '甘孜': 10127009, '广元': 10127010,
    '南充': 10127011, '内江': 10127013, '自贡': 10127014, '宜宾': 10127015,
    '泸州': 10127016, '攀枝花': 10127017, '德阳': 10127018, '资阳': 10127021,
    '眉山': 10127056, '广安': 10127070, '遂宁': 10127117, '巴中': 10127125,
    '达州': 10127137,
    '昆明': 10130001, '曲靖': 10130002, '昭通': 10130003, '文山': 10130005,
    '大理': 10130007, '楚雄': 10130008, '临沧': 10130009, '保山': 10130010,
    '玉溪': 10130011, '丽江': 10130030, '普洱': 10130052, '红河': 10130098,
    '西双版纳傣族自治州': 10130127, '德宏傣族景颇族自治州': 10130128,
    '怒江傈僳族自治州': 10130129, '迪庆藏族自治州': 10130130,
    '杭州': 10131001, '温州': 10131002, '宁波': 10131003, '绍兴': 10131004,
    '湖州': 10131005, '嘉兴': 10131006, '金华': 10131009, '丽水': 10131010,
    '衢州': 10131011, '台州': 10131012, '舟山': 10131015,
    '福州': 10107001, '厦门': 10107002, '泉州': 10107003, '南平': 10107004,
    '漳州': 10107007, '龙岩': 10107008, '三明': 10107009, '莆田': 10107010,
    '宁德': 10107013,
    '哈尔滨': 10114001, '绥化': 10114002, '佳木斯': 10114003, '牡丹江': 10114004,
    '齐齐哈尔': 10114005, '大庆': 10114007, '大兴安岭': 10114008, '鸡西': 10114009,
    '鹤岗': 10114020, '双鸭山': 10114024, '伊春': 10114042, '黑河': 10114053,
    '七台河': 10114067,
    '长春': 10117001, '吉林': 10117002, '通化': 10117004, '四平': 10117006,
    '白城': 10117007, '松原': 10117008, '辽源': 10117028, '白山': 10117048,
    '延边朝鲜族自治州': 10117049,
    '沈阳': 10120001, '铁岭': 10120002, '抚顺': 10120003, '鞍山': 10120004,
    '营口': 10120005, '大连': 10120006, '本溪': 10120007, '丹东': 10120008,
    '锦州': 10120009, '朝阳': 10120010, '阜新': 10120011, '盘锦': 10120012,
    '辽阳': 10120013, '葫芦岛': 10120014,
    '拉萨': 10128001, '那曲': 10128002, '昌都': 10128003, '山南': 10128004,
    '日喀则': 10128005, '阿里': 10128006, '林芝': 10128007,
    '乌鲁木齐': 10129001, '克拉玛依': 10129004, '阿勒泰': 10129006, '巴音郭楞': 10129007,
    '哈密': 10129008, '吐鲁番': 10129009, '阿克苏': 10129010, '喀什': 10129011,
    '和田': 10129012, '昌吉': 10129017, '塔城': 10129086,
    '克孜勒苏柯尔克孜自治州': 10129088, '博尔塔拉蒙古自治州': 10129089,
    '伊犁哈萨克自治州': 10129090, '自治区直辖县级': 10129091,
}

PROVINCE_MAP = {
    "北京": ["朝阳区","海淀区","丰台区","昌平区","通州区","大兴区","西城区","房山区","顺义区","东城区","石景山区","怀柔区","门头沟区","密云区","延庆区","平谷区"],
    "上海": ["闸北区","黄浦区","徐汇区","浦东新区","闵行区","宝山区","松江区","嘉定区","杨浦区","普陀区","青浦区","奉贤区","静安区","虹口区","金山区","长宁区","崇明区"],
    "广东": ["广州","深圳","佛山","湛江","珠海","肇庆","东莞","惠州","中山","茂名","汕头","梅州","韶关","江门","清远","潮州","阳江","河源","揭阳","汕尾","云浮"],
    "天津": ["和平区","河东区","河西区","南开区","河北区","红桥区","东丽区","西青区","滨海新区","津南区","武清区","宝坻区","静海区","宁河区","北辰区","蓟州区"],
    "重庆": ["渝中区","渝北区","合川区","九龙坡区","沙坪坝区","永川区","巴南区","南岸区","江北区","北碚区","璧山区","大渡口区","万州区","开州区","梁平区","黔江区","武隆区","江津区","涪陵区","綦江区","大足区","长寿区","荣昌区","铜梁区","潼南区","南川区","云阳县","奉节县","忠县","垫江县","丰都县","巫山县","巫溪县","城口县","石柱土家族自治县","酉阳土家族苗族自治县","彭水苗族土家族自治县","秀山土家族苗族自治县"],
    "安徽": ["合肥","淮南","蚌埠","宿州","阜阳","六安","滁州","芜湖","安庆","黄山","铜陵","淮北","亳州","马鞍山","池州","宣城"],
    "甘肃": ["兰州","张掖","武威","酒泉","金昌","天水","定西","平凉","甘南","嘉峪关","庆阳","白银","陇南","临夏"],
    "广西": ["南宁","柳州","钦州","百色","玉林","防城港","桂林","梧州","河池","北海","贵港","来宾","崇左","贺州"],
    "贵州": ["贵阳","六盘水","安顺","遵义","毕节","铜仁","黔西南布依族苗族自治州","黔东南苗族侗族自治州","黔南布依族苗族自治州"],
    "海南": ["海口","三亚","省直辖县级"],
    "河北": ["石家庄","衡水","邢台","邯郸","沧州","唐山","廊坊","秦皇岛","承德","保定","张家口"],
    "河南": ["郑州","新乡","安阳","许昌","驻马店","漯河","信阳","周口","洛阳","平顶山","三门峡","南阳","开封","商丘","鹤壁","濮阳","焦作","河南省直辖县级"],
    "湖北": ["武汉","黄石","鄂州","咸宁","十堰","宜昌","恩施","荆州","黄冈","荆门","孝感","襄阳","随州","湖北省直辖县级"],
    "湖南": ["长沙","株洲","益阳","岳阳","常德","娄底","怀化","衡阳","邵阳","郴州","张家界","湘潭","永州","湘西土家族苗族自治州"],
    "江苏": ["南京","苏州","无锡","徐州","常州","镇江","连云港","盐城","扬州","南通","淮安","泰州","宿迁"],
    "江西": ["南昌","九江","景德镇","上饶","鹰潭","宜春","萍乡","赣州","吉安","抚州","新余"],
    "内蒙古": ["呼和浩特","包头","乌海","赤峰","通辽","锡林郭勒盟","阿拉善盟","兴安","鄂尔多斯","呼伦贝尔","巴彦淖尔","乌兰察布"],
    "宁夏": ["银川","石嘴山","固原","吴忠","中卫"],
    "青海": ["西宁","果洛","玉树","海西","海东地区","海北藏族自治州","黄南藏族自治州","海南藏族自治州"],
    "山东": ["青岛","威海","济南","淄博","聊城","德州","东营","潍坊","烟台","泰安","菏泽","临沂","枣庄","济宁","日照","滨州","莱芜"],
    "山西": ["太原","忻州","大同","临汾","运城","阳泉","长治","晋城","朔州","晋中","吕梁"],
    "陕西": ["西安","渭南","延安","榆林","宝鸡","安康","汉中","铜川","咸阳","商洛"],
    "四川": ["成都","乐山","凉山","绵阳","阿坝","雅安","甘孜","广元","南充","内江","自贡","宜宾","泸州","攀枝花","德阳","资阳","眉山","广安","遂宁","巴中","达州"],
    "云南": ["昆明","曲靖","昭通","文山","大理","楚雄","临沧","保山","玉溪","丽江","普洱","红河","西双版纳傣族自治州","德宏傣族景颇族自治州","怒江傈僳族自治州","迪庆藏族自治州"],
    "浙江": ["杭州","温州","宁波","绍兴","湖州","嘉兴","金华","丽水","衢州","台州","舟山"],
    "福建": ["福州","厦门","泉州","南平","漳州","龙岩","三明","莆田","宁德"],
    "黑龙江": ["哈尔滨","绥化","佳木斯","牡丹江","齐齐哈尔","大庆","大兴安岭","鸡西","鹤岗","双鸭山","伊春","黑河","七台河"],
    "吉林": ["长春","吉林","通化","四平","白城","松原","辽源","白山","延边朝鲜族自治州"],
    "辽宁": ["沈阳","铁岭","抚顺","鞍山","营口","大连","本溪","丹东","锦州","朝阳","阜新","盘锦","辽阳","葫芦岛"],
    "西藏": ["拉萨","那曲","昌都","山南","日喀则","阿里","林芝"],
    "新疆": ["乌鲁木齐","克拉玛依","阿勒泰","巴音郭楞","哈密","吐鲁番","阿克苏","喀什","和田","昌吉","塔城","克孜勒苏柯尔克孜自治州","博尔塔拉蒙古自治州","伊犁哈萨克自治州","自治区直辖县级"],
}

# ========== SQLite persistence ==========
DB_LOCK = threading.RLock()
MEMBER_COLUMNS = {
    "ID": "member_id", "昵称": "nickname", "性别": "gender", "年龄": "age",
    "居住地": "city", "月收入": "income", "学历": "education", "婚姻状况": "marriage",
    "标签": "tags", "独白": "monologue", "头像链接": "avatar_url",
    "最新动态时间": "latest_moment_time", "实名认证时间": "certification_time",
    "最早动态时间": "earliest_moment_time", "VIP": "vip", "紫图可见": "purple_visible",
}


def db_connect():
    connection = sqlite3.connect(DB_PATH, timeout=30)
    connection.row_factory = sqlite3.Row
    return connection


def init_db():
    Path(BASE_DIR).mkdir(parents=True, exist_ok=True)
    with DB_LOCK, db_connect() as connection:
        connection.execute("PRAGMA journal_mode=DELETE")
        connection.execute("PRAGMA synchronous=NORMAL")
        connection.executescript("""
            CREATE TABLE IF NOT EXISTS members (
                member_id TEXT PRIMARY KEY, nickname TEXT, gender TEXT, age TEXT, city TEXT,
                income TEXT, education TEXT, marriage TEXT, tags TEXT, monologue TEXT,
                avatar_url TEXT, latest_moment_time TEXT, certification_time TEXT,
                earliest_moment_time TEXT, vip TEXT, purple_visible TEXT,
                inserted_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS pushed_ids (
                member_id TEXT PRIMARY KEY, pushed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS runtime_state (
                state_key TEXT PRIMARY KEY, state_value TEXT NOT NULL, updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS site_assets (
                asset_key TEXT PRIMARY KEY, content BLOB NOT NULL, content_type TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_members_certification ON members(certification_time);
            CREATE INDEX IF NOT EXISTS idx_members_purple ON members(purple_visible);
        """)


def get_state(key, default=""):
    with DB_LOCK, db_connect() as connection:
        row = connection.execute("SELECT state_value FROM runtime_state WHERE state_key = ?", (key,)).fetchone()
    return row[0] if row else default


def set_state(key, value):
    with DB_LOCK, db_connect() as connection:
        connection.execute(
            "INSERT INTO runtime_state(state_key, state_value, updated_at) VALUES(?, ?, CURRENT_TIMESTAMP) "
            "ON CONFLICT(state_key) DO UPDATE SET state_value=excluded.state_value, updated_at=CURRENT_TIMESTAMP",
            (key, str(value)),
        )


def delete_state(key):
    with DB_LOCK, db_connect() as connection:
        connection.execute("DELETE FROM runtime_state WHERE state_key = ?", (key,))


def get_site_asset(asset_key):
    with DB_LOCK, db_connect() as connection:
        row = connection.execute(
            "SELECT content, content_type FROM site_assets WHERE asset_key = ?", (asset_key,)
        ).fetchone()
    return (row["content"], row["content_type"]) if row else (None, None)


def load_scraped_ids():
    scraped = set()
    if os.path.exists(SCRAPED_IDS_FILE):
        try:
            with open(SCRAPED_IDS_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        scraped.add(line)
        except OSError as error:
            print(f"读取历史ID缓存失败: {error}")
    return scraped


def save_scraped_id(member_id):
    try:
        with open(SCRAPED_IDS_FILE, "a", encoding="utf-8") as f:
            f.write(f"{member_id}\n")
    except OSError as error:
        print(f"保存历史ID缓存失败: {error}")


def upsert_member_record(record):
    values = {db_col: str(record.get(csv_name, "") or "") for csv_name, db_col in MEMBER_COLUMNS.items()}
    if not values["member_id"]:
        return
    columns = ", ".join(values)
    placeholders = ", ".join("?" for _ in values)
    update_set = ", ".join(f"{column} = excluded.{column}" for column in values if column != "member_id")
    with DB_LOCK, db_connect() as connection:
        connection.execute(
            f"INSERT INTO members ({columns}) VALUES ({placeholders}) "
            f"ON CONFLICT(member_id) DO UPDATE SET {update_set}",
            tuple(values.values()),
        )


def migrate_legacy_files():
    """Import previous CSV and state files exactly once before legacy files are archived."""
    if get_state("legacy_migration_complete") == "1":
        return
    legacy_members = Path(BASE_DIR) / "zhenai_members.csv"
    legacy_pushed = Path(BASE_DIR) / ".pushed_cert_ids.txt"
    legacy_progress = Path(BASE_DIR) / "city_progress.txt"
    legacy_snapshot = Path(BASE_DIR) / "daily_snapshot.json"
    imported_members = imported_pushed = 0
    try:
        if legacy_members.exists():
            with legacy_members.open("r", encoding="utf-8-sig", newline="") as handle:
                for record in csv.DictReader(handle):
                    if (record.get("ID") or "").strip():
                        upsert_member_record(record)
                        imported_members += 1
        if legacy_pushed.exists():
            with DB_LOCK, db_connect() as connection:
                for member_id in legacy_pushed.read_text(encoding="utf-8").splitlines():
                    if member_id.strip():
                        connection.execute("INSERT OR IGNORE INTO pushed_ids(member_id) VALUES(?)", (member_id.strip(),))
                        imported_pushed += 1
        if legacy_progress.exists() and legacy_progress.read_text(encoding="utf-8").strip():
            set_state("city_progress", legacy_progress.read_text(encoding="utf-8").strip())
        if legacy_snapshot.exists():
            snapshot = json.loads(legacy_snapshot.read_text(encoding="utf-8"))
            if isinstance(snapshot, dict) and snapshot.get("date"):
                set_state("daily_snapshot", json.dumps(snapshot, ensure_ascii=False))
    except (OSError, ValueError, csv.Error) as error:
        raise RuntimeError(f"旧数据迁移失败，未切换服务：{error}") from error
    set_state("legacy_migration_complete", "1")
    print(f"SQLite 数据迁移完成：会员{imported_members}，已推送 ID {imported_pushed}")


def _get_main_total():
    with DB_LOCK, db_connect() as connection:
        return int(connection.execute("SELECT COUNT(*) FROM members").fetchone()[0])


def _get_current_city():
    return get_state("city_progress", "未开始") or "未开始"


def _get_checkpoint_label():
    current_city = _get_current_city()
    for municipality in ("北京", "上海", "天津", "重庆"):
        if current_city in PROVINCE_MAP.get(municipality, []):
            return municipality
    return current_city


def _format_elapsed(seconds):
    seconds = max(0, int(seconds))
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    if hours:
        return f"{hours}小时{minutes}分"
    if minutes:
        return f"{minutes}分{seconds}秒"
    return f"{seconds}秒"


def _get_today_start_total(total):
    today = datetime.now().strftime("%Y-%m-%d")
    try:
        snapshot = json.loads(get_state("daily_snapshot", "{}"))
        if snapshot.get("date") == today:
            return int(snapshot.get("start_total", total))
    except (ValueError, TypeError):
        pass
    set_state("daily_snapshot", json.dumps({"date": today, "start_total": total}, ensure_ascii=False))
    return total


# =============================================================================
# 爬虫核心类
# =============================================================================

class ZhenaiCrawler:
    def __init__(self, token, seedtoken):
        self.token = token
        self.seedtoken = seedtoken
        self.session = requests.Session()
        self.last_search_error_code = None

    def generate_ua(self, channel=925536, sub_channel=3, version="9.13.0", ver_num="52", token=None):
        """动态生成符合珍爱网小程序安全校验的 ua 字符串与哈希"""
        t = str(uuid.uuid4())
        a = token if token else (self.token if self.token else "^CE%JUm#r3w&)")
        i = a + f"{version}{ver_num}" + t
        r = 5381
        for char in i:
            r = (r + (r << 5) + ord(char)) & 0xFFFFFFFF
        hash_val = 2147483647 & r
        return f"h5/{version}/{ver_num}/0/0/0/{channel}/{sub_channel}/0/0/0/{t}/0/0/{hash_val}"

    def _common_headers(self, req_ua):
        return {
            "ua": req_ua,
            "xweb_xhr": "1",
            "seedtoken": self.seedtoken,
            "cookie": f"token={self.token}" if self.token else "token=",
            "user-agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 "
                "MicroMessenger/7.0.20.1781(0x6700143B) NetType/WIFI "
                "MiniProgramEnv/Windows WindowsWechat/WMPF "
                "WindowsWechat(0x63090a13) UnifiedPCLinuxWechat(0xf2741107) XWEB/14978"
            ),
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "referer": "https://servicewechat.com/wxeb13e85bef8b60d9/426/page-frame.html",
            "accept": "*/*",
        }

    def search_members(self, page=1, page_size=20,
                       age_begin=-1, age_end=-1,
                       work_city=-1, gender=1,
                       salary_begin=-1, salary_end=-1, hometown_city=-1):
        if not self.token:
            print("⚠️ 搜索模式必须填入登录 token，当前为游客态，已跳过。")
            return [], False

        self.last_search_error_code = None
        url = "https://api.zhenai.com/search/getConditionData.do"
        req_ua = self.generate_ua()
        payload = {
            "ageBegin": age_begin, "ageEnd": age_end,
            "heightBegin": 129, "heightEnd": -1,
            "salaryBegin": salary_begin, "salaryEnd": salary_end,
            "multiEducation": "", "multiWorkCity": work_city,
            "multiMarriage": -1, "occupation": -1,
            "multiHomeTown": hometown_city, "multiHouse": -1,
            "car": -1, "multiChildren": -1, "multiConstellation": -1,
            "avatar": -1, "zhenxin": 1, "identityOk": -1,
            "loginType": 3, "pageSize": page_size, "page": page, "ua": req_ua,
        }
        try:
            resp = self.session.post(url, data=payload, headers=self._common_headers(req_ua))
            if resp.status_code == 200:
                data = resp.json()
                if data.get("isError"):
                    self.last_search_error_code = str(data.get("errorCode", ""))
                    print(f"  搜索接口报错: {data.get('errorMessage')} (code={data.get('errorCode')})")
                    return [], False
                inner = data.get("data", {})
                return inner.get("list", []), inner.get("hasNext", False)
            print(f"搜索接口 HTTP 错误: {resp.status_code} {resp.text[:200]}")
        except Exception as e:
            print(f"搜索接口网络异常: {e}")
        return [], False

    def get_member_profile(self, object_id):
        url = "https://api.zhenai.com/profile/getObjectProfileForWeChat.do"
        req_ua = self.generate_ua()
        params = {"objectID": object_id, "wxApp": 1, "location": 0, "_method": "GET", "ua": req_ua}
        try:
            resp = self.session.get(url, params=params, headers=self._common_headers(req_ua))
            if resp.status_code == 200:
                return resp.json().get("data", {})
            print(f"获取会员 {object_id} 资料失败: {resp.status_code}")
        except Exception as e:
            print(f"获取会员 {object_id} 资料网络异常: {e}")
        return {}

    def parse_profile(self, info):
        if not info:
            return None
        tags = []
        for section in info.get("basicDetailsList", []):
            for tag_item in section.get("detailListV2", []):
                tags.append(tag_item.get("tagName"))
        return {
            "ID": info.get("memberID", ""),
            "昵称": info.get("nickname", ""),
            "性别": "女" if info.get("gender") == 1 else "男",
            "年龄": info.get("age", ""),
            "居住地": info.get("workCityString", ""),
            "月收入": info.get("salaryString", ""),
            "学历": info.get("educationString", ""),
            "婚姻状况": info.get("marriageString", ""),
            "标签": " | ".join(tags),
            "独白": info.get("introduce", {}).get("monologue", ""),
            "头像链接": info.get("avatarURL", ""),
            "VIP": "是" if info.get("isZhenXinVip") else "否",
        }

    def get_moments(self, object_id):
        """获取指定会员第一页动态（limit 10），不再翻页。"""
        req_ua = self.generate_ua()
        url = "https://api.zhenai.com/moment/getPersonalMomentsByWeChat.do"
        params = {
            "objectID": object_id, "limit": 10,
            "timestamp": 0, "momentType": 0,
            "_method": "GET", "ua": req_ua,
        }
        try:
            resp = self.session.get(url, params=params, headers=self._common_headers(req_ua))
            if resp.status_code == 200:
                return resp.json().get("data", {}).get("list", []) or []
        except Exception as e:
            print(f"    获取 {object_id} 动态网络异常: {e}")
        return []

    def has_purple_panel(self, cert_ts, now=None):
        """
        紫图可见（新判定）：
        只需要实名认证时间落在「当前实时时间向前 48 小时」窗口内即为是。
        有没有动态都不影响判定；48 小时外一律判否。
        """
        if not cert_ts:
            return False
        reference_now = time.time() if now is None else float(now)
        certification_seconds = float(cert_ts) / 1000
        window_start = reference_now - CERTIFICATION_WINDOW_HOURS * 3600
        return window_start <= certification_seconds <= reference_now

    def parse_moments_times(self, moments, object_id=None, check_purple=True):
        """解析动态时间（最新、实名认证、最早）+ 紫图资格。"""
        if not moments:
            return "", "", "", False

        def fmt(ts):
            return datetime.fromtimestamp(ts / 1000, BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S") if ts else ""

        latest = fmt(moments[0].get("moment", {}).get("createTime"))
        cert = ""
        cert_ts = 0
        for m in moments:
            if m.get("text") == "已完成人脸认证" or m.get("moment", {}).get("type") == 6:
                timestamp = m.get("moment", {}).get("createTime", 0) or 0
                if timestamp > cert_ts:
                    cert_ts = timestamp
        if cert_ts:
            cert = fmt(cert_ts)

        has_recent = False
        if check_purple and cert_ts > 0:
            has_recent = self.has_purple_panel(cert_ts)

        earliest = fmt(moments[-1].get("moment", {}).get("createTime")) if not cert else ""
        return latest, cert, earliest, has_recent


# =============================================================================
# 公共：处理单个会员 ID（查详情 → 过滤 → 抓动态 → 写库）
# =============================================================================

def income_meets_minimum(income_text, minimum_income):
    """Return whether a salary band's lower bound meets the configured minimum."""
    if not minimum_income:
        return True
    text = str(income_text or "").replace(",", "").replace("，", "")
    values = [int(value) for value in re.findall(r"\d+", text)]
    if not values:
        return False
    if "以下" in text:
        return False
    return values[0] >= minimum_income


def process_member(crawler, mid, scraped_ids,
                   gender_filter=None, city_filter=None,
                   extra_filters=None, cert_days=0):
    """
    处理单个会员 ID。
    cert_days>0: 先查动态 → 实名不在 N 天内直接跳过（省 1 次详情 API）

    处理顺序：
      1. 查 scraped_ids 缓存 → 命中直接跳过
      2. cert_days>0 时，先拉动态拿实名时间，实名缺失 / 过期 → 写缓存后跳过（不写库）
      3. 拉详情 profile，解析字段
      4. 无论后续过滤是否通过，立即写入 scraped_ids 缓存
      5. 城市 / 性别 / 学历 / 婚姻过滤
      6. 动态时间 + 紫图判定
      7. 通过后 upsert 进 members 表
    """
    mid_str = str(mid)
    if mid_str in scraped_ids:
        return False

    # ====== 快速模式：先查动态拿实名时间，过期直接跳过 ======
    pre_moments = []
    if cert_days > 0:
        pre_moments = crawler.get_moments(mid)
        _, cert_time, _, _ = crawler.parse_moments_times(
            pre_moments, object_id=mid, check_purple=False
        )
        if not cert_time or not certification_within_window(cert_time, hours=cert_days * 24):
            scraped_ids.add(mid_str)
            save_scraped_id(mid_str)
            return False

    raw_info = crawler.get_member_profile(mid)
    parsed = crawler.parse_profile(raw_info)

    scraped_ids.add(mid_str)
    save_scraped_id(mid_str)

    if not parsed:
        print(f"    解析会员 {mid} 失败或返回空")
        return False

    if city_filter and city_filter not in parsed["居住地"]:
        print(f"    跳过: {parsed['昵称']} (居住地: {parsed['居住地']}，不含'{city_filter}')")
        return False

    if gender_filter and parsed["性别"] != gender_filter:
        print(f"    跳过: {parsed['昵称']} (性别: {parsed['性别']}，不符合'{gender_filter}')")
        return False

    if extra_filters:
        _, edu_f, marriage_f = extra_filters
        if edu_f and parsed.get("学历", "") != edu_f:
            print(f"    跳过: {parsed['昵称']} (学历: {parsed.get('学历')}，不符合'{edu_f}')")
            return False
        if marriage_f and parsed.get("婚姻状况", "") != marriage_f:
            print(f"    跳过: {parsed['昵称']} (婚姻: {parsed.get('婚姻状况')}，不符合'{marriage_f}')")
            return False

    if cert_days > 0:
        moments = pre_moments
    else:
        moments = crawler.get_moments(mid)
    latest, cert, earliest, has_real_moment = crawler.parse_moments_times(
        moments, object_id=mid
    )
    parsed["最新动态时间"] = latest
    parsed["实名认证时间"] = cert
    parsed["最早动态时间"] = earliest
    parsed["VIP"] = parsed.get("VIP", "否")
    parsed["紫图可见"] = "是" if has_real_moment else "否"

    upsert_member_record(parsed)
    print(
        f"    🎉 写入: {parsed['昵称']} | {parsed['性别']} | {parsed['年龄']}岁 | "
        f"{parsed['居住地']} | 💰{parsed.get('月收入', '')} | 🎓{parsed.get('学历', '')} | 💍{parsed.get('婚姻状况', '')} | "
        f"最新动态:{latest or '无'} | 实名:{cert or '无'} | 紫图:{parsed['紫图可见']}"
    )
    return True


# ========== Flask dashboard ==========
app = Flask(__name__)
app.secret_key = DASHBOARD_SESSION_SECRET or hashlib.sha256(
    (DASHBOARD_ACCESS_TOKEN + "|ton-man-dashboard-session").encode("utf-8")
).hexdigest()
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SECURE=True,
    SESSION_COOKIE_SAMESITE="Strict",
)
LOG_HISTORY_LIMIT = 240
SUBSCRIBER_QUEUE_LIMIT = 256
log_history = []
log_subscribers = set()
log_broadcast_lock = threading.RLock()
crawl_running = threading.Event()
manual_stop_requested = threading.Event()
start_lock = threading.Lock()
crawl_thread = None
current_stats = {"total": 0, "by_province": {}}
auto_restart_lock = threading.Lock()
auto_restart_timer = None
auto_restart_generation = 0


def emit(message):
    """Broadcast one log event to every connected dashboard view."""
    message = str(message)
    set_state("last_log_at", str(time.time()))
    with log_broadcast_lock:
        log_history.append(message)
        del log_history[:-LOG_HISTORY_LIMIT]
        for subscriber in tuple(log_subscribers):
            try:
                subscriber.put_nowait(message)
            except queue.Full:
                try:
                    subscriber.get_nowait()
                    subscriber.put_nowait(message)
                except queue.Empty:
                    pass


def clear_live_log_history():
    with log_broadcast_lock:
        log_history.clear()


class LogCapture(io.StringIO):
    def write(self, value):
        super().write(value)
        for line in value.split("\n"):
            if line.strip():
                emit(line.strip())
        return len(value)

    def flush(self):
        pass


def send_start_report(age_begin, age_end, gender, cert_days):
    if not TG_BOT_TOKEN:
        return
    now = datetime.now(BEIJING_TZ)
    message = "\n".join((
        "『 状 态 正 常 』",
        "━━━━━━━━━━━━━━━━",
        f"城市断点：{_get_checkpoint_label()}",
        "",
        f"年龄要求：{age_begin}-{age_end}岁",
        "",
        "━━━━━━━━━━━━━━━━",
        f"时间：{now.month}-{now.strftime('%d %H：%M')}",
    ))
    for chat_id in dict.fromkeys(((TG_CHAT_ID if TG_PRIVATE_PUSH_ENABLED else ""), GROUP_CHAT_ID)):
        if not chat_id:
            continue
        try:
            response = requests.post(
                f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
                json={"chat_id": chat_id, "text": message}, timeout=10,
            )
            if response.ok and os.environ.get("AUTO_DELETE_START_REPORT", "1") != "0":
                try:
                    message_id = response.json().get("result", {}).get("message_id")
                    if message_id:
                        time.sleep(_env_int("START_REPORT_DELETE_DELAY_SECONDS", 3))
                        delete_response = requests.post(
                            f"https://api.telegram.org/bot{TG_BOT_TOKEN}/deleteMessage",
                            json={"chat_id": chat_id, "message_id": message_id}, timeout=10,
                        )
                        if not delete_response.ok:
                            emit(f"启动报备自动撤回失败：HTTP {delete_response.status_code}")
                except (ValueError, TypeError, requests.RequestException):
                    emit("启动报备自动撤回失败，不影响采集任务。")
        except requests.RequestException:
            emit("启动报备发送失败，不影响采集任务。")


def get_location_mode():
    mode = get_state("location_mode", "work")
    return mode if mode in {"work", "hometown"} else "work"


def next_location_mode(mode):
    return "hometown" if mode == "work" else "work"


def location_mode_label(mode):
    return "工作地区" if mode == "work" else "籍贯地区"


INCOME_OPTIONS = (0, 3000, 5000, 8000, 12000)
PUSH_WINDOW_OPTIONS = (24, 48)
AGE_OPTIONS = ((40, 65), (45, 65), (50, 65), (55, 65))
PUSH_AGE_BEGIN = _env_int("PUSH_AGE_BEGIN", 45)
PUSH_AGE_END = _env_int("PUSH_AGE_END", 60)
MIN_SELECTED_PROVINCES = _env_int("MIN_SELECTED_PROVINCES", 15)


def selectable_provinces():
    return sorted(province for province in PROVINCE_MAP if province not in EXCLUDED_PROVINCES)


def default_run_filter_config():
    return {
        "provinces": selectable_provinces(),
        "age_begin": DEFAULT_SEARCH_CONFIG["age_begin"],
        "age_end": DEFAULT_SEARCH_CONFIG["age_end"],
        "minimum_income": DEFAULT_SEARCH_CONFIG["minimum_income"],
        "push_window_hours": DEFAULT_SEARCH_CONFIG["push_window_hours"],
    }


def get_run_filter_config():
    defaults = default_run_filter_config()
    try:
        saved = json.loads(get_state("run_filter_config", ""))
    except (TypeError, ValueError, json.JSONDecodeError):
        saved = {}
    selected_provinces = saved.get("provinces") if isinstance(saved, dict) else None
    valid_provinces = set(selectable_provinces())
    provinces = [p for p in selected_provinces or defaults["provinces"] if p in valid_provinces]
    if not provinces:
        provinces = defaults["provinces"]
    try:
        age_begin = int(saved.get("age_begin", defaults["age_begin"]))
        age_end = int(saved.get("age_end", defaults["age_end"]))
    except (TypeError, ValueError):
        age_begin, age_end = defaults["age_begin"], defaults["age_end"]
    minimum_income = saved.get("minimum_income", defaults["minimum_income"]) if isinstance(saved, dict) else defaults["minimum_income"]
    push_window_hours = saved.get("push_window_hours", defaults["push_window_hours"]) if isinstance(saved, dict) else defaults["push_window_hours"]
    try:
        minimum_income = int(minimum_income)
    except (TypeError, ValueError):
        minimum_income = defaults["minimum_income"]
    try:
        push_window_hours = int(push_window_hours)
    except (TypeError, ValueError):
        push_window_hours = defaults["push_window_hours"]
    return {
        "provinces": provinces,
        "age_begin": age_begin if (age_begin, age_end) in AGE_OPTIONS else defaults["age_begin"],
        "age_end": age_end if (age_begin, age_end) in AGE_OPTIONS else defaults["age_end"],
        "minimum_income": minimum_income if minimum_income in INCOME_OPTIONS else defaults["minimum_income"],
        "push_window_hours": push_window_hours if push_window_hours in PUSH_WINDOW_OPTIONS else defaults["push_window_hours"],
    }


def validate_run_filter_config(payload):
    if not isinstance(payload, dict):
        raise ValueError("筛选配置格式无效")
    available = set(selectable_provinces())
    provinces = payload.get("provinces")
    if not isinstance(provinces, list):
        raise ValueError("请选择至少一个省份")
    provinces = list(dict.fromkeys(str(province) for province in provinces))
    if any(province not in available for province in provinces):
        raise ValueError("省份选项无效")
    if len(provinces) < MIN_SELECTED_PROVINCES:
        raise ValueError(f"请至少选择 {MIN_SELECTED_PROVINCES} 个省份")
    try:
        age_begin = int(payload.get("age_begin", DEFAULT_SEARCH_CONFIG["age_begin"]))
        age_end = int(payload.get("age_end", DEFAULT_SEARCH_CONFIG["age_end"]))
    except (TypeError, ValueError):
        raise ValueError("年龄范围无效") from None
    try:
        minimum_income = int(payload.get("minimum_income"))
    except (TypeError, ValueError):
        raise ValueError("工资选项无效") from None
    try:
        push_window_hours = int(payload.get("push_window_hours"))
    except (TypeError, ValueError):
        raise ValueError("群推送时间窗无效") from None
    if (age_begin, age_end) not in AGE_OPTIONS:
        raise ValueError("年龄范围无效")
    if minimum_income not in INCOME_OPTIONS or push_window_hours not in PUSH_WINDOW_OPTIONS:
        raise ValueError("筛选选项无效")
    return {
        "provinces": [p for p in selectable_provinces() if p in set(provinces)],
        "age_begin": age_begin,
        "age_end": age_end,
        "minimum_income": minimum_income,
        "push_window_hours": push_window_hours,
    }


def save_run_filter_config(config):
    set_state("run_filter_config", json.dumps(config, ensure_ascii=False))


def cancel_auto_restart(announce=False):
    global auto_restart_timer, auto_restart_generation
    with auto_restart_lock:
        auto_restart_generation += 1
        if auto_restart_timer is not None:
            auto_restart_timer.cancel()
            auto_restart_timer = None
        delete_state("auto_restart_at")
    if announce:
        emit("⏹️ 已取消下一轮自动启动。")


def _launch_crawl(auto_started=False):
    global crawl_thread
    if crawl_running.is_set():
        return False, "已有采集任务在运行"
    clear_live_log_history()
    filters = get_run_filter_config()
    provinces = filters["provinces"]
    age_begin = filters["age_begin"]
    age_end = filters["age_end"]
    gender = DEFAULT_SEARCH_CONFIG["gender"]
    max_pages = DEFAULT_SEARCH_CONFIG["max_pages"]
    extra = (
        DEFAULT_SEARCH_CONFIG["income_filter"],
        DEFAULT_SEARCH_CONFIG["education_filter"],
        DEFAULT_SEARCH_CONFIG["marriage_filter"],
        DEFAULT_SEARCH_CONFIG["cert_days"],
    )
    location_mode = get_location_mode()
    _get_today_start_total(_get_main_total())
    manual_stop_requested.clear()
    set_state("crawl_intent", "running")
    set_state("runtime_started_at", str(time.time()))
    set_state("runtime_last_progress_at", str(time.time()))
    set_state("runtime_current_page", "0")
    crawl_running.set()
    delete_state("auto_restart_at")
    if auto_started:
        emit(f"🔄 等待结束，已自动启动新一轮{location_mode_label(location_mode)}采集。")
    else:
        emit(f"开始搜索采集 | {'、'.join(provinces)} | {gender} | {age_begin}-{age_end} 岁 | 最多 {max_pages} 页")
    emit(f"固定规则：近 {extra[3] * 24} 小时实名认证即判紫图")
    emit(f"群推送规则：月收入 {filters['minimum_income']} 元以上")
    crawl_thread = threading.Thread(
        target=crawl_worker,
        args=(provinces, age_begin, age_end, gender, max_pages, extra, location_mode),
        daemon=True,
    )
    crawl_thread.start()
    threading.Thread(target=send_start_report, args=(age_begin, age_end, gender, extra[3]), daemon=True).start()
    return True, ""


def schedule_auto_restart(delay_seconds=AUTO_RESTART_DELAY_SECONDS, restoring=False):
    global auto_restart_timer, auto_restart_generation
    if not AUTO_RESTART_ENABLED:
        return
    delay_seconds = max(0, int(delay_seconds))
    restart_at = time.time() + delay_seconds
    with auto_restart_lock:
        auto_restart_generation += 1
        generation = auto_restart_generation
        if auto_restart_timer is not None:
            auto_restart_timer.cancel()
        set_state("auto_restart_at", str(restart_at))

        def restart_when_due():
            global auto_restart_timer
            with auto_restart_lock:
                if generation != auto_restart_generation:
                    return
                auto_restart_timer = None
            with start_lock:
                if generation != auto_restart_generation or crawl_running.is_set():
                    return
                _launch_crawl(auto_started=True)

        auto_restart_timer = threading.Timer(delay_seconds, restart_when_due)
        auto_restart_timer.daemon = True
        auto_restart_timer.start()
    if restoring:
        emit("🔄 已恢复自动启动倒计时。")
    else:
        emit(f"⏲️ 本轮已完成；{location_mode_label(next_location_mode(get_location_mode()))}将在下一轮开始。")


def auto_restart_remaining_seconds():
    try:
        return max(0, int(float(get_state("auto_restart_at", "0")) - time.time()))
    except (TypeError, ValueError):
        return 0


def resume_auto_restart_if_pending():
    if not AUTO_RESTART_ENABLED or crawl_running.is_set():
        return
    remaining = auto_restart_remaining_seconds()
    if remaining:
        schedule_auto_restart(remaining, restoring=True)
    elif get_state("auto_restart_at", ""):
        schedule_auto_restart(0, restoring=True)


def resume_crawl_if_intended():
    if get_state("crawl_intent", "") != "running" or crawl_running.is_set():
        return
    if get_state("auto_restart_at", ""):
        return
    with start_lock:
        if not crawl_running.is_set():
            _launch_crawl(auto_started=True)


def run_search(provinces, age_begin, age_end, gender, max_pages, extra=("", "", "", 0), location_mode="work"):
    crawler = ZhenaiCrawler(token=MY_TOKEN, seedtoken=MY_SEED)
    scraped = load_scraped_ids()
    gender_value = 1 if gender == "女" else 2
    if any(province in ("全国", "全國") for province in provinces):
        provinces = list(PROVINCE_MAP)

    cities = []
    for province in provinces:
        if province in EXCLUDED_PROVINCES:
            continue
        for city_name in PROVINCE_MAP.get(province, []):
            city_code = CITY_CODE_MAP.get(city_name)
            if city_code is not None:
                cities.append((city_name, city_code, province))
    if not cities:
        emit("❌ 没有可执行的城市任务。")
        return False
    emit(f"🗺️ {location_mode_label(location_mode)}城市级覆盖：共 {len(cities)} 个城市，已排除福建。")

    total_new = 0
    current_stats["total"] = 0
    current_stats["by_province"] = {}
    last_city = get_state("city_progress", "")
    valid_cities = {name for name, _, _ in cities}
    if last_city and last_city not in valid_cities:
        emit(f"⚠️ 旧断点“{last_city}”不在新的城市级任务中，将从首个城市重新开始。")
        last_city = ""
    skip_mode = bool(last_city)
    if skip_mode:
        emit(f"⏩ 续传模式：跳过已完成的 {last_city} 之前的城市...")

    for city_name, city_code, province in cities:
        if not crawl_running.is_set():
            emit("⏹️ 暂停信号已确认，停止后续城市。")
            break
        if skip_mode:
            if city_name == last_city:
                skip_mode = False
            else:
                continue

        emit(f"\n📍 {city_name} ...")
        set_state("city_progress", city_name)
        set_state("runtime_last_progress_at", str(time.time()))
        city_new = 0
        for page in range(1, max_pages + 1):
            if not crawl_running.is_set():
                break
            time.sleep(CRAWL_DELAY_SECONDS)
            if not crawl_running.is_set():
                break
            members, has_next = crawler.search_members(
                page=page, page_size=20, age_begin=age_begin, age_end=age_end,
                work_city=city_code if location_mode == "work" else -1,
                hometown_city=city_code if location_mode == "hometown" else -1,
                gender=gender_value, salary_begin=-1,
            )
            set_state("runtime_current_page", str(page))
            set_state("runtime_last_progress_at", str(time.time()))
            if crawler.last_search_error_code == "-00004":
                emit("❌ 登录状态已过期，已停止采集；请更新 MY_TOKEN 后再启动。")
                crawl_running.clear()
                break
            if not members:
                break
            ids = [m.get("memberId") or m.get("memberID") or m.get("objectID") for m in members]
            ids = [member_id for member_id in ids if member_id]
            new_ids = [member_id for member_id in ids if str(member_id) not in scraped]
            emit(f"  📄 第{page}页: {len(ids)}人, 新{len(new_ids)}人")
            for member_id in ids:
                if str(member_id) in scraped or not crawl_running.is_set():
                    continue
                if process_member(
                    crawler, member_id, scraped,
                    extra_filters=extra[:3], cert_days=extra[3],
                ):
                    city_new += 1
                    total_new += 1
                time.sleep(CRAWL_DELAY_SECONDS)
            if not has_next or not crawl_running.is_set():
                break
            time.sleep(CRAWL_DELAY_SECONDS)
        current_stats.setdefault("by_province", {}).setdefault(province, {})[city_name] = city_new
        current_stats["total"] = total_new
        emit(f"  ✅ {city_name}: +{city_new} | 累计: {total_new}")
        set_state("city_progress", city_name)
        emit("__STATS__:" + json.dumps(current_stats, ensure_ascii=False))
        if not crawl_running.is_set():
            emit("⏹️ 采集已暂停，已保留当前城市进度。")
            break
    if crawl_running.is_set():
        delete_state("city_progress")
        emit(f"\n🎉 完成！共新增 {total_new} 条")
        return True
    return False


def crawl_worker(provinces, age_begin, age_end, gender, max_pages, extra, location_mode="work"):
    old_stdout = sys.stdout
    completed_normally = False
    sys.stdout = LogCapture()
    try:
        completed_normally = run_search(provinces, age_begin, age_end, gender, max_pages, extra, location_mode)
    except Exception as error:
        emit(f"❌ 错误: {error}")
    finally:
        sys.stdout = old_stdout
        crawl_running.clear()
        if completed_normally and not manual_stop_requested.is_set():
            next_mode = next_location_mode(location_mode)
            set_state("location_mode", next_mode)
            emit(f"✅ {location_mode_label(location_mode)}完整遍历结束；下一轮切换为{location_mode_label(next_mode)}。")
            if AUTO_RESTART_ENABLED:
                schedule_auto_restart()
            else:
                set_state("crawl_intent", "stopped")
        emit("__DONE__")


def tg_daily_push_reporter():
    """Send yesterday's successful group-push count at Beijing midnight."""
    last_report_date = get_state("last_daily_push_report_date", "")
    while True:
        now = datetime.now(BEIJING_TZ)
        report_date = (now - timedelta(days=1)).strftime("%Y-%m-%d")
        next_midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=5, microsecond=0)
        if now.hour == 0 and now.minute == 0 and now.second >= 5:
            next_midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=5, microsecond=0)
        if report_date != last_report_date and now.hour == 0 and now.minute == 0 and TG_BOT_TOKEN and GROUP_CHAT_ID:
            try:
                with DB_LOCK, db_connect() as connection:
                    collected_count = connection.execute(
                        "SELECT COUNT(*) FROM members WHERE date(datetime(inserted_at, '+8 hours')) = ?", (report_date,)
                    ).fetchone()[0]
                    pushed_count = connection.execute(
                        "SELECT COUNT(*) FROM pushed_ids WHERE date(datetime(pushed_at, '+8 hours')) = ?", (report_date,)
                    ).fetchone()[0]
                text = "\n".join((
                    "📊 昨日群推送统计", "━━━━━━━━━━",
                    f"日期：{report_date}",
                    f"当天采集：{collected_count} 条",
                    f"成功推送：{pushed_count} 条",
                    "━━━━━━━━━━", "机器人：TopMan_ZiYuan_bot",
                ))
                response = requests.post(
                    f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
                    json={"chat_id": GROUP_CHAT_ID, "text": text}, timeout=15,
                )
                if telegram_response_ok(response, "sendMessage"):
                    set_state("last_daily_push_report_date", report_date)
                    emit(f"📊 每日采集/推送统计已发送：{report_date}，采集{collected_count}条，推送{pushed_count}条")
            except (OSError, sqlite3.Error, requests.RequestException) as error:
                emit(f"每日群推送统计发送失败：{error}")
        time.sleep(max(1, int((next_midnight - datetime.now(BEIJING_TZ)).total_seconds())))


# ========== Telegram profile card pushes ==========
def unique_nonempty(values):
    seen = set()
    result = []
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def tags_from_sections(sections):
    values = []
    for section in sections or []:
        for item in section.get("detailListV2", []) or []:
            values.append(item.get("tagName"))
        for item in section.get("detailList", []) or []:
            if isinstance(item, dict):
                values.append(item.get("tagName") or item.get("name") or item.get("value"))
            else:
                values.append(item)
    return unique_nonempty(values)


def profile_card_tags(profile):
    basic_seed = [
        f"{profile.get('age')}岁" if profile.get("age") not in (None, "") else "",
        profile.get("marriageString"),
        profile.get("heightString"),
        profile.get("workCityString"),
        profile.get("workProvinceCityString"),
        profile.get("occupation"),
        profile.get("salaryString"),
        profile.get("educationString"),
    ]
    object_seed = [
        profile.get("objectAgeString"),
        profile.get("objectMarriageString"),
        profile.get("objectHeightString"),
        profile.get("objectWorkCityString"),
        profile.get("objectEducationString"),
        profile.get("objectSalaryString"),
        profile.get("objectWantChildrenString"),
        profile.get("objectChildrenString"),
    ]
    basic_tags = unique_nonempty(basic_seed + tags_from_sections(profile.get("basicDetailsList")))
    partner_tags = unique_nonempty(object_seed + tags_from_sections(profile.get("objectDetailsList")))
    return basic_tags, partner_tags


def find_font(size, bold=False):
    candidates = list(FONT_CANDIDATES)
    if bold:
        candidates.insert(0, "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc")
    for font_path in candidates:
        if os.path.exists(font_path):
            return ImageFont.truetype(font_path, size=size, index=0)
    return ImageFont.load_default()


def text_box(draw, text, font):
    left, top, right, bottom = draw.textbbox((0, 0), text, font=font)
    return right - left, bottom - top


def trim_tag(draw, text, font, max_width):
    if text_box(draw, text, font)[0] <= max_width:
        return text
    suffix = "…"
    trimmed = text
    while trimmed and text_box(draw, trimmed + suffix, font)[0] > max_width:
        trimmed = trimmed[:-1]
    return (trimmed + suffix) if trimmed else suffix


def layout_tag_rows(draw, tags, font, max_width):
    rows = []
    row = []
    used_width = 0
    pill_gap = 16
    horizontal_padding = 25
    for raw_tag in tags or ["暂无公开资料"]:
        tag = trim_tag(draw, raw_tag, font, max_width - horizontal_padding * 2)
        tag_width, _ = text_box(draw, tag, font)
        pill_width = tag_width + horizontal_padding * 2
        if row and used_width + pill_gap + pill_width > max_width:
            rows.append(row)
            row = []
            used_width = 0
        if row:
            used_width += pill_gap
        row.append((tag, pill_width))
        used_width += pill_width
    if row:
        rows.append(row)
    return rows


def draw_tag_section(draw, y, title, tags, title_font, tag_font, content_width):
    page_left = 58
    title_height = text_box(draw, title, title_font)[1]
    draw.text((page_left, y), title, font=title_font, fill="#101828")
    y += title_height + 28

    rows = layout_tag_rows(draw, tags or ["暂无公开择偶条件"], tag_font, content_width)
    _, tag_height = text_box(draw, "资料", tag_font)
    pill_height = max(tag_height + 24, 58)
    for row in rows:
        x = page_left
        for tag, pill_width in row:
            draw.rounded_rectangle(
                (x, y, x + pill_width, y + pill_height),
                radius=pill_height // 2, fill="#F2F4F7",
            )
            draw.ellipse((x + 17, y + pill_height // 2 - 4, x + 25, y + pill_height // 2 + 4), fill="#64748B")
            draw.text((x + 36, y + (pill_height - tag_height) // 2 - 2), tag, font=tag_font, fill="#1D2939")
            x += pill_width + 16
        y += pill_height + 18
    return y


def render_profile_card(profile, output_path):
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    width = 1080
    page_left = 58
    content_width = width - page_left * 2
    title_font = find_font(50, bold=True)
    tag_font = find_font(34)

    measure = Image.new("RGB", (width, 200), "white")
    measure_draw = ImageDraw.Draw(measure)
    basic_tags, partner_tags = profile_card_tags(profile)
    y = 64
    y = draw_tag_section(measure_draw, y, "基本资料", basic_tags, title_font, tag_font, content_width)
    y += 54
    y = draw_tag_section(measure_draw, y, "择偶条件", partner_tags, title_font, tag_font, content_width)
    height = max(y + 56, 480)

    image = Image.new("RGB", (width, height), "#FFFFFF")
    draw = ImageDraw.Draw(image)
    y = 64
    y = draw_tag_section(draw, y, "基本资料", basic_tags, title_font, tag_font, content_width)
    y += 54
    draw.line((page_left, y - 27, width - page_left, y - 27), fill="#EAECF0", width=2)
    draw_tag_section(draw, y, "择偶条件", partner_tags, title_font, tag_font, content_width)
    image.save(output_path, format="PNG", optimize=True)
    return output_path


def parse_beijing_certification_time(certification_time):
    try:
        return datetime.strptime((certification_time or "")[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=BEIJING_TZ)
    except (TypeError, ValueError):
        return None


def certification_within_window(certification_time, hours=CERTIFICATION_WINDOW_HOURS, now=None):
    certification_dt = parse_beijing_certification_time(certification_time)
    if certification_dt is None:
        return False
    reference_now = now or datetime.now(BEIJING_TZ)
    if reference_now.tzinfo is None:
        reference_now = reference_now.replace(tzinfo=BEIJING_TZ)
    else:
        reference_now = reference_now.astimezone(BEIJING_TZ)
    return reference_now - timedelta(hours=hours) <= certification_dt <= reference_now


def age_in_push_range(age_text):
    try:
        age = int(str(age_text or "").strip())
    except (TypeError, ValueError):
        return False
    return PUSH_AGE_BEGIN <= age <= PUSH_AGE_END


def recent_candidates():
    run_config = get_run_filter_config()
    push_window_hours = run_config["push_window_hours"]
    minimum_income = run_config["minimum_income"]
    with DB_LOCK, db_connect() as connection:
        rows = [dict(row) for row in connection.execute(
            "SELECT * FROM members WHERE purple_visible = '是' "
            "AND member_id NOT IN (SELECT member_id FROM pushed_ids)"
        ).fetchall()]
    return [
        row for row in rows
        if age_in_push_range(row.get("age"))
        and certification_within_window(row.get("certification_time"), hours=push_window_hours)
        and income_meets_minimum(row.get("income", ""), minimum_income)
    ]


def day_label(certification_time):
    date_text = (certification_time or "")[:10]
    today = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d")
    yesterday = (datetime.now(BEIJING_TZ) - timedelta(days=1)).strftime("%Y-%m-%d")
    return "今天" if date_text == today else "昨天" if date_text == yesterday else "前天"


def text_caption(user):
    return "\n".join((
        f"🔔 {day_label(user.get('certification_time', ''))}新实名·紫图",
        f"ID:{user.get('member_id', '')} | {user.get('nickname', '')} | {user.get('age', '')}岁 | {user.get('city', '')} | {user.get('income', '')}",
        f"实名:{user.get('certification_time', '')}",
    ))


def card_caption(user):
    return " | ".join((
        f"ID:{user.get('member_id', '')}",
        str(user.get("nickname", "") or ""),
        f"{user.get('age', '')}岁",
        str(user.get("city", "") or ""),
    ))


def telegram_response_ok(response, method):
    if response.ok:
        return True
    description = ""
    try:
        description = str(response.json().get("description", ""))
    except (ValueError, AttributeError):
        pass
    emit(f"Telegram {method} 发送失败：HTTP {response.status_code}{' · ' + description if description else ''}")
    return False


def send_text(caption):
    response = requests.post(
        f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
        json={"chat_id": GROUP_CHAT_ID, "text": caption}, timeout=20,
    )
    return telegram_response_ok(response, "sendMessage")


def send_card_to_chat(chat_id, card_path, caption):
    with open(card_path, "rb") as card_file:
        response = requests.post(
            f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendPhoto",
            data={"chat_id": chat_id, "caption": caption},
            files={"photo": (Path(card_path).name, card_file, "image/png")}, timeout=45,
        )
    return telegram_response_ok(response, "sendPhoto")


def send_card(card_path, caption):
    return send_card_to_chat(GROUP_CHAT_ID, card_path, caption)


def get_avatar_url(profile, user):
    for value in (
        profile.get("avatarURL"), profile.get("avatarUrl"), profile.get("avatar"),
        user.get("avatar_url"),
    ):
        url = str(value or "").strip()
        if url.startswith(("https://", "http://")):
            return url
    return ""


def download_avatar(avatar_url):
    if not avatar_url:
        return None
    try:
        response = requests.get(avatar_url, timeout=(5, 12))
    except requests.RequestException as error:
        emit(f"头像下载超时或失败，已改为只推资料卡：{type(error).__name__}")
        return None
    if not response.ok or len(response.content) == 0 or len(response.content) > 8 * 1024 * 1024:
        return None
    descriptor, avatar_path = tempfile.mkstemp(prefix="zhenai_avatar_", suffix=".jpg")
    try:
        with Image.open(io.BytesIO(response.content)) as avatar:
            normalized = avatar.convert("RGB")
            with os.fdopen(descriptor, "wb") as avatar_file:
                normalized.save(avatar_file, format="JPEG", quality=90, optimize=True)
    except (OSError, ValueError):
        try:
            os.close(descriptor)
        except OSError:
            pass
        try:
            os.unlink(avatar_path)
        except OSError:
            pass
        return None
    return avatar_path


def send_profile_album_to_chat(chat_id, avatar_path, card_path, caption):
    media = [
        {"type": "photo", "media": "attach://avatar", "caption": caption},
        {"type": "photo", "media": "attach://profile_card"},
    ]
    with open(avatar_path, "rb") as avatar_file, open(card_path, "rb") as card_file:
        response = requests.post(
            f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMediaGroup",
            data={"chat_id": chat_id, "media": json.dumps(media, ensure_ascii=False)},
            files={
                "avatar": ("avatar.jpg", avatar_file, "image/jpeg"),
                "profile_card": ("profile_card.png", card_file, "image/png"),
            },
            timeout=60,
        )
    return telegram_response_ok(response, "sendMediaGroup")


def send_profile_album(avatar_path, card_path, caption):
    return send_profile_album_to_chat(GROUP_CHAT_ID, avatar_path, card_path, caption)


def telegram_profile_lookup(chat_id, member_ids):
    if not SCREENSHOT_TOKEN:
        telegram_control_reply(chat_id, "⚠️ 未配置资料查询凭据，暂时无法生成资料卡。")
        return
    crawler = ZhenaiCrawler(token=SCREENSHOT_TOKEN, seedtoken=SCREENSHOT_SEED)
    for member_id in member_ids:
        temporary_card = temporary_avatar = None
        try:
            profile = crawler.get_member_profile(member_id)
            if not profile:
                telegram_control_reply(chat_id, f"⚠️ ID {member_id} 未获取到用户资料。")
                continue
            parsed = crawler.parse_profile(profile) or {}
            descriptor, temporary_card = tempfile.mkstemp(prefix="zhenai_lookup_card_", suffix=".png")
            os.close(descriptor)
            render_profile_card(profile, temporary_card)
            temporary_avatar = download_avatar(get_avatar_url(profile, {"avatar_url": parsed.get("头像链接", "")}))
            caption = card_caption({
                "member_id": member_id,
                "nickname": parsed.get("昵称", ""),
                "age": parsed.get("年龄", ""),
                "city": parsed.get("居住地", ""),
            })
            if temporary_avatar:
                sent = send_profile_album_to_chat(chat_id, temporary_avatar, temporary_card, caption)
            else:
                sent = send_card_to_chat(chat_id, temporary_card, caption)
            if not sent:
                telegram_control_reply(chat_id, f"⚠️ ID {member_id} 发送失败。")
        except (OSError, ValueError, requests.RequestException) as error:
            emit(f"私人资料查询失败：{type(error).__name__}")
            telegram_control_reply(chat_id, f"⚠️ ID {member_id} 查询失败。")
        finally:
            for path in (temporary_card, temporary_avatar):
                if path:
                    try:
                        os.unlink(path)
                    except OSError:
                        pass


def push_new_candidates():
    if not TG_BOT_TOKEN or not GROUP_CHAT_ID:
        return
    push_window_hours = get_run_filter_config()["push_window_hours"]
    candidates = recent_candidates()
    if not candidates:
        return
    card_crawler = ZhenaiCrawler(token=SCREENSHOT_TOKEN, seedtoken=SCREENSHOT_SEED) if TG_PROFILE_CARD_ENABLED else None
    successful_ids = []
    for user in candidates:
        fallback_caption = text_caption(user)
        image_caption = card_caption(user)
        temporary_card = None
        temporary_avatar = None
        try:
            if card_crawler is None:
                sent = send_text(fallback_caption)
            else:
                profile = card_crawler.get_member_profile(user["member_id"])
                if profile:
                    moments = card_crawler.get_moments(user["member_id"])
                    _, fresh_certification, _, fresh_purple = card_crawler.parse_moments_times(moments)
                    if not certification_within_window(fresh_certification, hours=push_window_hours) or not fresh_purple:
                        with DB_LOCK, db_connect() as connection:
                            connection.execute(
                                "UPDATE members SET purple_visible = '否' WHERE member_id = ?",
                                (user["member_id"],),
                            )
                        emit(f"紫图复核未通过，跳过推送：{user['member_id']}")
                        continue
                    descriptor, temporary_card = tempfile.mkstemp(prefix="zhenai_card_", suffix=".png")
                    os.close(descriptor)
                    render_profile_card(profile, temporary_card)
                    temporary_avatar = download_avatar(get_avatar_url(profile, user))
                    if temporary_avatar:
                        sent = send_profile_album(temporary_avatar, temporary_card, image_caption)
                    else:
                        sent = send_card(temporary_card, image_caption)
                else:
                    sent = send_text(fallback_caption)
            if sent:
                successful_ids.append(user["member_id"])
        except (OSError, ValueError, requests.RequestException) as error:
            emit(f"资料卡推送失败：{error}")
        finally:
            if temporary_card:
                try:
                    os.unlink(temporary_card)
                except OSError:
                    pass
            if temporary_avatar:
                try:
                    os.unlink(temporary_avatar)
                except OSError:
                    pass
    if successful_ids:
        with DB_LOCK, db_connect() as connection:
            for member_id in successful_ids:
                connection.execute("INSERT OR IGNORE INTO pushed_ids(member_id) VALUES(?)", (member_id,))
        emit(f"📨 复合筛选 TG 图片卡推送：{len(successful_ids)} 条")


def group_push_reporter():
    time.sleep(10)
    while True:
        try:
            push_new_candidates()
        except (OSError, sqlite3.Error, requests.RequestException) as error:
            emit(f"复合筛选推送检查失败：{error}")
        time.sleep(PUSH_INTERVAL_SECONDS)


def telegram_control_reply(chat_id, text):
    if not TG_BOT_TOKEN or not chat_id:
        return
    try:
        response = requests.post(
            f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
            json={"chat_id": chat_id, "text": text}, timeout=20,
        )
        telegram_response_ok(response, "control.sendMessage")
    except requests.RequestException as error:
        emit(f"Telegram 控制回复失败：{type(error).__name__}")


def format_group_stats_report():
    now = datetime.now(BEIJING_TZ)
    today = now.strftime("%Y-%m-%d")
    yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    with DB_LOCK, db_connect() as connection:
        today_pushed = connection.execute(
            "SELECT COUNT(*) FROM pushed_ids WHERE date(datetime(pushed_at, '+8 hours')) = ?", (today,)
        ).fetchone()[0]
        yesterday_pushed = connection.execute(
            "SELECT COUNT(*) FROM pushed_ids WHERE date(datetime(pushed_at, '+8 hours')) = ?", (yesterday,)
        ).fetchone()[0]
        total_pushed = connection.execute("SELECT COUNT(DISTINCT member_id) FROM pushed_ids").fetchone()[0]
    return "\n".join((
        "━━━━━━━━━━", "『 八 方 来 财 』", "━━━━━━━━━━",
        f"💭 当日实时：{today_pushed}条",
        f"📅 昨日推送：{yesterday_pushed}条",
        "━━━━━━━━━━",
        f"总合：{total_pushed}条",
        f"北京时间：{now:%m月%d日 %H:%M}",
    ))


def build_runtime_status():
    now = time.time()
    running = crawl_running.is_set()
    started_at = float(get_state("runtime_started_at", "0") or 0)
    last_progress_at = float(get_state("runtime_last_progress_at", "0") or 0)
    last_log_at = float(get_state("last_log_at", "0") or 0)
    progress_age = int(now - last_progress_at) if last_progress_at else None
    if not running:
        health = "已停止"
    elif progress_age is not None and progress_age > 180:
        health = "需留意：超过 3 分钟无页面进度"
    else:
        health = "正常"
    filters = get_run_filter_config()
    return {
        "running": running,
        "health": health,
        "provinces": filters["provinces"],
        "age_begin": DEFAULT_SEARCH_CONFIG["age_begin"],
        "age_end": DEFAULT_SEARCH_CONFIG["age_end"],
        "city": _get_current_city(),
        "page": int(get_state("runtime_current_page", "0") or 0),
        "new_records": current_stats["total"],
        "elapsed_seconds": int(now - started_at) if running and started_at else 0,
        "last_progress_seconds": progress_age,
        "last_log_seconds": int(now - last_log_at) if last_log_at else None,
        "auto_restart_delay_seconds": AUTO_RESTART_DELAY_SECONDS,
        "auto_restart_in_seconds": 0 if running else auto_restart_remaining_seconds(),
    }


def format_runtime_status():
    status = build_runtime_status()
    state_icon = "🟢" if status["running"] and status["health"] == "正常" else ("🟡" if status["running"] else "⚪")
    progress = "暂无" if status["last_progress_seconds"] is None else f"{status['last_progress_seconds']}秒前"
    log_age = "暂无" if status["last_log_seconds"] is None else f"{status['last_log_seconds']}秒前"
    if status["running"]:
        auto_restart = f"本轮完成后等待{_format_elapsed(status['auto_restart_delay_seconds'])}"
    else:
        auto_restart = "未安排" if not status["auto_restart_in_seconds"] else _format_elapsed(status["auto_restart_in_seconds"])
    return "\n".join((
        f"{state_icon} 采集状态：{status['health']}",
        f"地区：{'、'.join(status['provinces'])}",
        f"年龄：{status['age_begin']}-{status['age_end']}岁",
        f"当前城市：{status['city']}",
        f"当前页：第 {status['page']} 页",
        f"本轮新增：{status['new_records']} 条",
        f"已运行：{_format_elapsed(status['elapsed_seconds']) if status['running'] else '未运行'}",
        f"最近进度：{progress}",
        f"最近日志：{log_age}",
        f"自动重启：{auto_restart}",
    ))


def telegram_control_reporter():
    if not TG_BOT_TOKEN or not TG_CONTROL_USER_ID:
        return
    offset = int(get_state("telegram_control_offset", "0") or 0)
    while True:
        try:
            response = requests.get(
                f"https://api.telegram.org/bot{TG_BOT_TOKEN}/getUpdates",
                params={"offset": offset, "timeout": 20, "allowed_updates": json.dumps(["message"])},
                timeout=35,
            )
            response.raise_for_status()
            updates = response.json().get("result", [])
            for update in updates:
                offset = max(offset, int(update.get("update_id", 0)) + 1)
                set_state("telegram_control_offset", str(offset))
                message = update.get("message") or {}
                chat = message.get("chat") or {}
                sender = message.get("from") or {}
                if chat.get("type") != "private" or str(sender.get("id", "")) != str(TG_CONTROL_USER_ID):
                    if (
                        chat.get("type") in {"group", "supergroup"}
                        and str(chat.get("id", "")) == str(GROUP_CHAT_ID)
                        and (
                            f"@{TG_BOT_USERNAME}".lower() in str(message.get("text", "")).lower()
                            or str(message.get("text", "")).strip().lower() in {"/stats", "/统计"}
                            or str(message.get("text", "")).strip().lower().startswith(f"/stats@{TG_BOT_USERNAME}".lower())
                        )
                    ):
                        try:
                            response = requests.post(
                                f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
                                json={"chat_id": chat.get("id"), "text": format_group_stats_report()}, timeout=15,
                            )
                            telegram_response_ok(response, "group.stats.sendMessage")
                        except (OSError, requests.RequestException, sqlite3.Error):
                            emit("群内统计发送失败，不影响采集任务。")
                    continue
                text = str(message.get("text", "") or "").strip()
                command = text.split()[0].lower() if text else ""
                reply_chat = str(TG_CONTROL_CHAT_ID or chat.get("id", ""))
                if command in {"/start", "/start_collect", "/开启采集", "开启采集"}:
                    with start_lock:
                        ok, detail = _launch_crawl()
                    age_begin = DEFAULT_SEARCH_CONFIG["age_begin"]
                    age_end = DEFAULT_SEARCH_CONFIG["age_end"]
                    provinces = "、".join(get_run_filter_config()["provinces"])
                    telegram_control_reply(reply_chat, f"✅ 已启动{provinces} {age_begin}–{age_end} 岁采集。" if ok else f"⚠️ 启动失败：{detail}")
                elif command in {"/stop", "/stop_collect", "/暂停采集", "/关闭采集", "暂停采集", "关闭采集"}:
                    cancel_auto_restart()
                    set_state("crawl_intent", "stopped")
                    manual_stop_requested.set()
                    crawl_running.clear()
                    telegram_control_reply(reply_chat, "⏹️ 已发送暂停请求，不会自动启动下一轮。")
                elif command in {"/status", "/状态", "状态"}:
                    telegram_control_reply(reply_chat, format_runtime_status())
                else:
                    member_ids = list(dict.fromkeys(re.findall(r"(?<!\d)\d{5,12}(?!\d)", text)))
                    if member_ids:
                        member_ids = member_ids[:20]
                        telegram_control_reply(reply_chat, f"🔎 已收到 {len(member_ids)} 个ID，正在查询资料卡和照片…")
                        threading.Thread(
                            target=telegram_profile_lookup,
                            args=(str(chat.get("id", reply_chat)), member_ids),
                            daemon=True, name="tg-private-profile-lookup",
                        ).start()
                    else:
                        telegram_control_reply(reply_chat, "可用命令：/start_collect 开启、/stop_collect 暂停、/status 状态；也可直接发送一个或多个用户ID查询资料卡和照片。")
        except (OSError, ValueError, requests.RequestException, json.JSONDecodeError) as error:
            emit(f"Telegram 私聊控制检查失败：{type(error).__name__}")
            time.sleep(10)


def dashboard_access_granted():
    return bool(DASHBOARD_ACCESS_TOKEN) and session.get("dashboard_access") is True


def require_dashboard_access(view):
    @wraps(view)
    def protected_view(*args, **kwargs):
        if dashboard_access_granted():
            return view(*args, **kwargs)
        return jsonify({"ok": False, "error": "需要先验证访问口令"}), 401
    return protected_view


@app.route("/cover-background.jpg")
def cover_background():
    content, content_type = get_site_asset("cover_background")
    if not content:
        return Response(status=404)
    return Response(content, mimetype=content_type, headers={"Cache-Control": "public, max-age=86400"})


@app.route("/dashboard-background.jpg")
@require_dashboard_access
def dashboard_background():
    content, content_type = get_site_asset("dashboard_background")
    if not content:
        return Response(status=404)
    return Response(content, mimetype=content_type, headers={"Cache-Control": "private, max-age=86400"})


@app.route("/login", methods=["POST"])
def login():
    payload = request.get_json(silent=True) or {}
    supplied_token = str(payload.get("token", ""))
    if not DASHBOARD_ACCESS_TOKEN or not hmac.compare_digest(supplied_token, DASHBOARD_ACCESS_TOKEN):
        return jsonify({"ok": False, "error": "口令无效"}), 401
    session.clear()
    session["dashboard_access"] = True
    return jsonify({"ok": True})


@app.route("/logout", methods=["POST"])
def logout():
    session.clear()
    return jsonify({"ok": True})


@app.route("/")
def index():
    return render_template_string(HTML_TEMPLATE if dashboard_access_granted() else COVER_TEMPLATE)


@app.route("/start", methods=["POST"])
@require_dashboard_access
def start():
    payload = request.get_json(silent=True)
    with start_lock:
        if crawl_running.is_set():
            return jsonify({"ok": False, "msg": "已有采集任务在运行"}), 409
        try:
            config = validate_run_filter_config(payload) if payload is not None else get_run_filter_config()
        except ValueError as error:
            return jsonify({"ok": False, "msg": str(error)}), 400
        save_run_filter_config(config)
        cancel_auto_restart()
        ok, message = _launch_crawl()
    return jsonify({"ok": ok, "msg": message})


@app.route("/api/run-config")
@require_dashboard_access
def api_run_config():
    return jsonify({
        "config": get_run_filter_config(),
        "provinces": selectable_provinces(),
        "age_options": AGE_OPTIONS,
        "income_options": INCOME_OPTIONS,
        "push_window_options": PUSH_WINDOW_OPTIONS,
    })


@app.route("/stop", methods=["POST"])
@require_dashboard_access
def stop():
    cancel_auto_restart()
    set_state("crawl_intent", "stopped")
    manual_stop_requested.set()
    crawl_running.clear()
    emit("⏹️ 已请求停止；不会自动启动下一轮。")
    return jsonify({"ok": True})


@app.route("/stream")
@require_dashboard_access
def stream():
    def generate():
        subscriber = queue.Queue(maxsize=SUBSCRIBER_QUEUE_LIMIT)
        with log_broadcast_lock:
            log_subscribers.add(subscriber)
            history = list(log_history)
        try:
            for message in history:
                if message.startswith("__STATS__:"):
                    yield f"event: stats\ndata: {message[9:]}\n\n"
                elif message != "__DONE__":
                    yield f"data: {json.dumps({'msg': message}, ensure_ascii=False)}\n\n"
            if current_stats["total"] > 0:
                yield f"event: stats\ndata: {json.dumps(current_stats, ensure_ascii=False)}\n\n"
            while True:
                try:
                    message = subscriber.get(timeout=1)
                    if message == "__DONE__":
                        yield 'data: {"done": true}\n\n'
                        break
                    if message.startswith("__STATS__:"):
                        yield f"event: stats\ndata: {message[9:]}\n\n"
                    else:
                        yield f"data: {json.dumps({'msg': message}, ensure_ascii=False)}\n\n"
                except queue.Empty:
                    yield 'data: {"ping": true}\n\n'
        finally:
            with log_broadcast_lock:
                log_subscribers.discard(subscriber)
    return Response(generate(), mimetype="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/status")
@require_dashboard_access
def status():
    return jsonify(build_runtime_status())


@app.route("/api/stats")
@require_dashboard_access
def api_stats():
    return jsonify({**current_stats, "all_total": _get_main_total()})


COVER_TEMPLATE = """
<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="robots" content="noindex, nofollow">
<title>TopMen · 访问验证</title>
<style>
:root { --ink: #030916; --gold: #d9a030; --gold-light: #f6d783; --red: #a81c1c; --paper: #f7e6be; }
* { box-sizing: border-box; }
html, body { width: 100%; min-height: 100%; margin: 0; }
body { min-height: 100vh; overflow: hidden; color: #fff7df; background: var(--ink); font-family: "Noto Sans SC", "Microsoft YaHei", sans-serif; }
.cover { min-height: 100vh; display: grid; place-items: center; position: relative; isolation: isolate; padding: 28px; background: radial-gradient(circle at 50% 48%, rgba(217, 160, 48, .17) 0%, rgba(168, 28, 28, .07) 43%, transparent 74%), #020817; }
.cover::before, .cover::after { content: ""; position: absolute; inset-inline: 0; height: 1px; z-index: -1; background: linear-gradient(90deg, transparent, rgba(217, 160, 48, .45), transparent); }
.cover::before { top: 0; } .cover::after { bottom: 0; opacity: .5; }
.stage { width: min(1120px, 100%); text-align: center; }
.frame { position: relative; aspect-ratio: 2.8 / 1; overflow: hidden; border: 4px solid rgba(154, 120, 55, .7); border-radius: 19px; background: #f7e6be; box-shadow: 0 28px 72px rgba(0,0,0,.58), 0 0 42px rgba(217,160,48,.16); cursor: pointer; transition: transform .24s ease, box-shadow .24s ease; }
.frame:hover, .frame:focus-visible { transform: scale(1.006); box-shadow: 0 28px 72px rgba(0,0,0,.58), 0 0 54px rgba(217,160,48,.33); outline: none; }
.cover-art { display: block; width: 100%; height: 100%; object-fit: cover; object-position: center; }
.art-fallback { min-height: 320px; display: grid; place-items: center; color: #6a1616; background: linear-gradient(180deg, #f7e6be, #dcb164); font-family: serif; font-size: clamp(26px, 6vw, 62px); font-weight: 900; letter-spacing: .16em; }
.prompt { margin: 25px auto 0; min-height: 43px; display: inline-flex; align-items: center; justify-content: center; gap: 12px; padding: 10px 22px; border: 1px solid rgba(245, 197, 66, .58); border-radius: 999px; color: #f6d783; background: linear-gradient(90deg, rgba(217,160,48,.13), rgba(217,160,48,.28), rgba(217,160,48,.13)); font-size: 14px; font-weight: 700; letter-spacing: .15em; box-shadow: 0 0 24px rgba(245,197,66,.12); }
.fineprint { margin: 12px 0 0; color: #98a6bc; font: 12px/1.5 "SFMono-Regular", Consolas, monospace; letter-spacing: .05em; }
.dialog { display: none; width: min(430px, 100%); margin: 24px auto 0; padding: 25px; border: 1px solid rgba(217,160,48,.45); border-radius: 17px; background: rgba(8, 16, 29, .9); box-shadow: 0 24px 60px rgba(0,0,0,.5); text-align: left; backdrop-filter: blur(18px); }
.dialog.open { display: block; animation: appear .22s ease; }
.dialog h1 { margin: 0; color: #fff2cf; font-size: 20px; letter-spacing: .08em; }
.dialog p { margin: 9px 0 19px; color: #aab6c8; font-size: 13px; line-height: 1.65; }
.dialog label { display: block; margin: 0 0 8px; color: #eec86e; font-size: 13px; font-weight: 700; }
.dialog input { width: 100%; height: 46px; padding: 0 13px; border: 1px solid rgba(217,160,48,.48); border-radius: 9px; color: #fff8e5; background: rgba(2, 8, 23, .8); font: 15px "SFMono-Regular", Consolas, monospace; outline: none; }
.dialog input:focus { border-color: #f6d783; box-shadow: 0 0 0 3px rgba(217,160,48,.16); }
.dialog button { width: 100%; height: 46px; margin-top: 14px; border: 0; border-radius: 9px; color: #3c0c0c; background: linear-gradient(135deg, #f6d783, #d9a030); font-weight: 800; letter-spacing: .12em; cursor: pointer; }
.dialog button:hover { filter: brightness(1.06); }
.error { min-height: 20px; margin: 10px 0 0; color: #ffb4a9; font-size: 13px; }
@keyframes appear { from { opacity: 0; transform: translateY(8px); } to { opacity: 1; transform: translateY(0); } }
@media (max-width: 620px) { .cover { padding: 18px; } .frame { aspect-ratio: 2.1 / 1; border-width: 3px; border-radius: 13px; } .prompt { margin-top: 19px; padding: 9px 15px; font-size: 12px; letter-spacing: .08em; } .dialog { padding: 20px; } }
</style>
</head>
<body>
<main class="cover">
  <section class="stage" aria-label="TopMen 访问封面">
    <div class="frame" id="coverFrame" tabindex="0" role="button" aria-controls="accessDialog" aria-expanded="false">
      <img class="cover-art" src="/cover-background.jpg" alt="财源广进，八方进财，黄金万两" onerror="this.replaceWith(Object.assign(document.createElement('div'), {className: 'art-fallback', textContent: '八方进财'}))">
    </div>
    <button class="prompt" id="openDialog" type="button">点击封面 · 验证口令进入控制台</button>
    <p class="fineprint">TOPMEN · SEARCH COLLECTION MISSION CONTROL</p>
    <form class="dialog" id="accessDialog" novalidate>
      <h1>进入 TopMen 控制台</h1>
      <p>访问口令只会提交至看板服务端验证，并以浏览器会话方式保存。</p>
      <label for="accessToken">控制台访问口令</label>
      <input id="accessToken" type="password" autocomplete="current-password" required autofocus>
      <button id="submitToken" type="submit">验证并进入看板</button>
      <div class="error" id="error" role="alert"></div>
    </form>
  </section>
</main>
<script>
const dialog = document.getElementById('accessDialog');
const token = document.getElementById('accessToken');
function openDialog() { dialog.classList.add('open'); document.getElementById('coverFrame').setAttribute('aria-expanded', 'true'); token.focus(); }
document.getElementById('openDialog').addEventListener('click', openDialog);
document.getElementById('coverFrame').addEventListener('click', openDialog);
document.getElementById('coverFrame').addEventListener('keydown', event => { if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); openDialog(); } });
dialog.addEventListener('submit', async event => {
  event.preventDefault();
  const submit = document.getElementById('submitToken');
  const error = document.getElementById('error');
  submit.disabled = true; error.textContent = '';
  try {
    const response = await fetch('/login', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({token: token.value})});
    const result = await response.json();
    if (!response.ok || !result.ok) throw new Error(result.error || '验证失败');
    window.location.replace('/');
  } catch (exception) { error.textContent = exception.message; token.select(); submit.disabled = false; }
});
</script>
</body>
</html>
"""


HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>TON MAN 2.0 — 搜索采集看板</title>
<style>
:root {
  --ink: #13233a;
  --muted: #697586;
  --surface: #ffffff;
  --canvas: #edf2f7;
  --line: #d9e2ec;
  --primary: #0f766e;
  --primary-hover: #0b5c56;
  --danger: #b42318;
  --danger-hover: #8f1d15;
  --terminal: #101923;
  --terminal-text: #dce8f5;
}
* { box-sizing: border-box; }
html, body { width: 100%; height: 100%; margin: 0; overflow: hidden; }
body {
  height: 100dvh;
  background: var(--canvas);
  color: var(--ink);
  font-family: "Noto Sans SC", "Microsoft YaHei", sans-serif;
}
.header {
  height: 72px;
  min-height: 72px;
  padding: 14px clamp(24px, 4vw, 64px);
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 24px;
  background: var(--ink);
  color: #fff;
}
.header h1 { margin: 0; font-size: clamp(22px, 2vw, 30px); letter-spacing: 0.04em; }
.status { flex: 0 0 auto; padding: 8px 13px; border: 1px solid rgba(255,255,255,.2); border-radius: 999px; font-size: 13px; }
.status.idle { color: #d5f5ef; background: rgba(15,118,110,.22); }
.status.running { color: #ffe7bd; background: rgba(180,83,9,.28); }
	.dashboard {
	  position: relative;
	  isolation: isolate;
	  height: calc(100dvh - 72px);
	  min-height: 0;
	  overflow: hidden;
	  padding: clamp(14px, 2vw, 28px) clamp(24px, 4vw, 64px);
	  display: grid;
	  grid-template-rows: min-content minmax(0, 1fr) auto;
	  gap: 12px;
}
.dashboard::before {
  content: "";
  position: absolute;
  inset: 0;
  z-index: -2;
  pointer-events: none;
  opacity: .24;
  transform: scale(1.035);
  background: #eee7dc url('/dashboard-background.jpg') center / cover no-repeat;
  filter: blur(10px) saturate(.85);
}
.dashboard::after {
  content: "";
  position: absolute;
  inset: 0;
  z-index: -1;
  pointer-events: none;
  opacity: .5;
  background: url('/dashboard-background.jpg') center / contain no-repeat;
}
.dashboard > * { position: relative; z-index: 1; }
		.config-panel { display: grid; grid-template-columns: minmax(280px, 1.65fr) minmax(230px, .95fr); gap: 10px; min-height: 0; padding: 10px 12px; border: 1px solid rgba(167,139,250,.54); border-radius: 12px; background: rgba(237,233,254,.88); backdrop-filter: blur(6px); box-shadow: 0 10px 24px rgba(76,29,149,.12); }
	.config-group { min-width: 0; }
	.config-heading { margin: 0 0 7px; color: var(--ink); font-size: 12px; font-weight: 800; letter-spacing: .08em; }
	.config-meta { margin-left: 7px; color: var(--muted); font-size: 11px; font-weight: 500; letter-spacing: normal; }
	.province-tools { display: flex; align-items: center; gap: 6px; margin: 0 0 7px; }
		.tiny-btn { min-height: 25px; padding: 3px 8px; border: 1px solid #c4b5fd; border-radius: 6px; color: #4c1d95; background: rgba(221,214,254,.58); font-size: 11px; font-weight: 700; cursor: pointer; }
		.tiny-btn:hover:not(:disabled) { border-color: #7c3aed; color: #5b21b6; background: rgba(196,181,253,.72); }
	.selection-summary { margin-left: auto; color: var(--muted); font-size: 11px; white-space: nowrap; }
		.province-list { display: grid; grid-auto-flow: column; grid-template-rows: repeat(5, minmax(22px, 1fr)); grid-auto-columns: minmax(82px, 1fr); gap: 5px; max-height: 135px; padding-right: 4px; overflow-x: auto; overflow-y: hidden; }
	.province-option, .choice-option { display: inline-flex; align-items: center; gap: 5px; min-width: 0; color: #334155; font-size: 12px; line-height: 1.2; cursor: pointer; }
		.province-option { min-height: 22px; padding: 3px 5px; border: 1px solid rgba(196,181,253,.75); border-radius: 6px; background: rgba(237,233,254,.76); }
		.province-option:has(input:checked) { color: #4c1d95; border-color: rgba(124,58,237,.6); background: rgba(221,214,254,.8); }
		.config-side { display: grid; grid-template-columns: 1fr; align-content: center; gap: 11px; padding-left: 11px; border-left: 1px solid rgba(167,139,250,.4); }
	.choice-row { display: flex; flex-wrap: wrap; gap: 7px; }
		.choice-option { min-height: 28px; padding: 5px 8px; border: 1px solid rgba(196,181,253,.86); border-radius: 7px; background: rgba(237,233,254,.82); }
		.choice-option:has(input:checked) { color: #4c1d95; border-color: rgba(124,58,237,.68); background: rgba(221,214,254,.86); font-weight: 700; }
		.config-panel input { accent-color: #7c3aed; }
	.config-panel input:disabled, .config-panel label:has(input:disabled), .config-panel button:disabled { cursor: not-allowed; opacity: .52; }
.log-panel {
  min-height: 0;
  display: flex;
  flex-direction: column;
  overflow: hidden;
  background: rgba(16, 25, 35, .91);
  border: 1px solid #263849;
  border-radius: 14px;
  box-shadow: 0 16px 38px rgba(15, 35, 58, .12);
}
.log-title { min-height: 58px; padding: 9px 13px 9px 18px; display: flex; align-items: center; justify-content: space-between; gap: 16px; color: #9eb5ca; border-bottom: 1px solid #263849; font-size: 13px; letter-spacing: .08em; }
.log-heading { flex: 1 1 auto; min-width: 0; }
.log-area { flex: 1; min-height: 0; overflow-y: auto; padding: 16px 20px 20px; color: var(--terminal-text); font-family: "SFMono-Regular", Consolas, monospace; font-size: 14px; line-height: 1.7; }
.log-area .line { white-space: pre-wrap; word-break: break-word; }
.actions { display: flex; flex: 0 0 auto; align-items: center; gap: 8px; }
.btn { min-width: 112px; min-height: 38px; padding: 8px 14px; border: 1px solid transparent; border-radius: 8px; color: #fff; cursor: pointer; font-size: 13px; font-weight: 800; letter-spacing: .04em; transition: transform .15s ease, background .15s ease, opacity .15s ease; }
.btn:hover:not(:disabled) { transform: translateY(-1px); }
.btn:focus-visible { outline: 3px solid #7dd3fc; outline-offset: 3px; }
.btn:disabled { cursor: not-allowed; opacity: .45; }
.btn-start { background: var(--primary); }
.btn-start:hover:not(:disabled) { background: var(--primary-hover); }
.btn-stop { background: var(--danger); }
.btn-stop:hover:not(:disabled) { background: var(--danger-hover); }
	@media (max-width: 720px) {
	  .header { height: 62px; min-height: 62px; padding: 10px 18px; }
	  .header h1 { font-size: 21px; }
		  .dashboard { height: calc(100dvh - 62px); min-height: 0; padding: 10px 14px; gap: 10px; }
	  .log-panel { height: auto; }
	  .log-title { min-height: 50px; padding: 7px 9px 7px 13px; gap: 8px; }
	  .config-panel { grid-template-columns: 1fr; gap: 8px; padding: 9px; }
	  .config-side { grid-template-columns: 1fr 1fr; gap: 8px; padding: 8px 0 0; border-top: 1px solid #e2e8f0; border-left: 0; }
		  .province-list { grid-auto-flow: row; grid-template-rows: none; grid-template-columns: repeat(auto-fill, minmax(62px, 1fr)); max-height: 62px; overflow-x: hidden; overflow-y: auto; }
	  .choice-row { gap: 5px; }
	  .choice-option { padding: 4px 6px; font-size: 11px; }
	  .actions { display: flex; gap: 6px; }
	  .btn { min-width: 82px; min-height: 34px; padding: 7px 9px; font-size: 12px; }
}
</style>
</head>
<body>
<header class="header">
  <div>
    <h1>TON MAN 2.0</h1>
  </div>
  <span class="status idle" id="status">空闲</span>
</header>

<main class="dashboard">
	  <section class="config-panel" id="runConfig" aria-label="启动前采集配置">
	    <div class="config-group">
	      <p class="config-heading">采集省份 <span class="config-meta">福建已排除</span></p>
	      <div class="province-tools">
	        <button class="tiny-btn" id="selectAllProvinces" type="button">全选</button>
	        <button class="tiny-btn" id="clearProvinces" type="button">清空</button>
	        <span class="selection-summary" id="provinceSummary">加载中…</span>
	      </div>
	      <div class="province-list" id="provinceOptions" role="group" aria-label="选择采集省份"></div>
	    </div>
		    <div class="config-side">
		      <div class="config-group">
	        <p class="config-heading">年龄范围 <span class="config-meta">4档选择</span></p>
		        <div class="choice-row" id="ageOptions"></div>
		      </div>
		      <div class="config-group">
	        <p class="config-heading">工资要求 <span class="config-meta">最低月收入</span></p>
	        <div class="choice-row" id="incomeOptions"></div>
	      </div>
	      <div class="config-group">
	        <p class="config-heading">群推送要求 <span class="config-meta">实名认证时限</span></p>
	        <div class="choice-row" id="pushWindowOptions"></div>
	      </div>
	    </div>
	  </section>

	  <section class="log-panel" aria-label="采集运行日志">
	    <div class="log-title">
	      <span class="log-heading">运行日志</span>
	      <div class="actions" aria-label="采集控制">
	        <button class="btn btn-start" id="btnStart" type="button" onclick="startCrawl()">启动采集</button>
	        <button class="btn btn-stop" id="btnStop" type="button" onclick="stopCrawl()" disabled>暂停采集</button>
	      </div>
	    </div>
    <div class="log-area" id="log"><div class="line">等待启动采集任务…</div></div>
  </section>
</main>

<script>
let eventSource = null;
let configLoaded = false;
let currentRunning = false;

function selectedProvinces() {
  return Array.from(document.querySelectorAll('#provinceOptions input:checked')).map(input => input.value);
}

function updateProvinceSummary() {
  const selected = selectedProvinces().length;
  const total = document.querySelectorAll('#provinceOptions input').length;
  const valid = selected >= 15;
  document.getElementById('provinceSummary').textContent = selected + ' / ' + total + ' 个省份 · 至少 15 个';
  document.getElementById('provinceSummary').style.color = valid ? '' : '#b42318';
  document.getElementById('btnStart').disabled = currentRunning || !configLoaded || !valid;
}

function selectedRadioValue(containerId) {
  const selected = document.querySelector('#' + containerId + ' input:checked');
  return selected ? Number(selected.value) : null;
}

function renderRunConfig(data) {
  const config = data.config || {};
  const selected = new Set(config.provinces || []);
  const provinceContainer = document.getElementById('provinceOptions');
  provinceContainer.replaceChildren();
  for (const province of data.provinces || []) {
    const label = document.createElement('label');
    label.className = 'province-option';
    const input = document.createElement('input');
    input.type = 'checkbox';
    input.value = province;
    input.checked = selected.has(province);
    input.addEventListener('change', updateProvinceSummary);
    label.append(input, document.createTextNode(province));
    provinceContainer.appendChild(label);
  }
	  const incomeContainer = document.getElementById('incomeOptions');
	  const ageContainer = document.getElementById('ageOptions');
	  ageContainer.replaceChildren();
	  for (const range of data.age_options || []) {
	    const label = document.createElement('label');
	    label.className = 'choice-option';
	    const input = document.createElement('input');
	    input.type = 'radio'; input.name = 'ageRange'; input.value = range[0] + '-' + range[1];
	    input.dataset.begin = range[0]; input.dataset.end = range[1];
	    input.checked = Number(config.age_begin) === Number(range[0]) && Number(config.age_end) === Number(range[1]);
	    label.append(input, document.createTextNode(range[0] + '–' + range[1] + '岁'));
	    ageContainer.appendChild(label);
	  }



  incomeContainer.replaceChildren();
  for (const income of data.income_options || []) {
    const label = document.createElement('label');
    label.className = 'choice-option';
    const input = document.createElement('input');
    input.type = 'radio'; input.name = 'minimumIncome'; input.value = income;
    input.checked = Number(config.minimum_income) === Number(income);
    label.append(input, document.createTextNode(Number(income) === 0 ? '不限（0）' : income + ' 元+'));
    incomeContainer.appendChild(label);
  }
  const windowContainer = document.getElementById('pushWindowOptions');
  windowContainer.replaceChildren();
  for (const hours of data.push_window_options || []) {
    const label = document.createElement('label');
    label.className = 'choice-option';
    const input = document.createElement('input');
    input.type = 'radio'; input.name = 'pushWindowHours'; input.value = hours;
    input.checked = Number(config.push_window_hours) === Number(hours);
    label.append(input, document.createTextNode(hours + ' 小时'));
    windowContainer.appendChild(label);
  }
  configLoaded = true;
  updateProvinceSummary();
  setRunning(currentRunning);
}

document.getElementById('selectAllProvinces').addEventListener('click', () => {
  document.querySelectorAll('#provinceOptions input').forEach(input => { input.checked = true; });
  updateProvinceSummary();
});
document.getElementById('clearProvinces').addEventListener('click', () => {
  document.querySelectorAll('#provinceOptions input').forEach(input => { input.checked = false; });
  updateProvinceSummary();
});

function log(message) {
  const line = document.createElement('div');
  line.className = 'line';
  line.textContent = message;
  const logEl = document.getElementById('log');
  logEl.appendChild(line);
  logEl.scrollTop = logEl.scrollHeight;
}

function setRunning(running) {
  currentRunning = running;
  document.getElementById('btnStart').disabled = running || !configLoaded || selectedProvinces().length < 15;
  document.getElementById('btnStop').disabled = !running;
  document.querySelectorAll('#runConfig input, #runConfig button').forEach(element => { element.disabled = running; });
  const status = document.getElementById('status');
  status.textContent = running ? '运行中' : '空闲';
  status.className = 'status ' + (running ? 'running' : 'idle');
}

function closeStream() {
  if (eventSource) {
    eventSource.close();
    eventSource = null;
  }
}

function connectStream() {
  closeStream();
  eventSource = new EventSource('/stream');
  eventSource.onmessage = event => {
    const data = JSON.parse(event.data);
    if (data.done) {
      log('采集任务已结束');
      closeStream();
      setRunning(false);
    } else if (data.msg) {
      log(data.msg);
    }
  };
  eventSource.onerror = () => {
    if (eventSource && eventSource.readyState === EventSource.CLOSED) {
      fetch('/status').then(response => response.json()).then(status => {
        setRunning(status.running);
        if (status.running) setTimeout(connectStream, 1000);
        else closeStream();
      }).catch(() => {});
    }
  };
}

async function startCrawl() {
  const config = {
    provinces: selectedProvinces(),
    minimum_income: selectedRadioValue('incomeOptions'),
    push_window_hours: selectedRadioValue('pushWindowOptions'),
    age_begin: Number(document.querySelector('#ageOptions input:checked')?.dataset.begin),
    age_end: Number(document.querySelector('#ageOptions input:checked')?.dataset.end),
  };
  if (config.provinces.length < 15) {
    log('无法启动：请至少选择 15 个省份。');
    return;
  }
  try {
    const response = await fetch('/start', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(config),
    });
    const result = await response.json();
    if (!result.ok) {
      log('无法启动：' + result.msg);
      return;
    }
    document.getElementById('log').replaceChildren();
    setRunning(true);
    log('已发送启动请求');
    connectStream();
  } catch (error) {
    log('启动请求失败：' + error.message);
  }
}

async function stopCrawl() {
  try {
    const response = await fetch('/stop', { method: 'POST' });
    const result = await response.json();
    if (result.ok) log('已请求暂停，正在安全结束当前任务…');
  } catch (error) {
    log('暂停请求失败：' + error.message);
  }
}

fetch('/api/run-config').then(response => {
  if (!response.ok) throw new Error('无法读取启动配置');
  return response.json();
}).then(renderRunConfig).catch(error => log('配置加载失败：' + error.message));
fetch('/status').then(response => response.json()).then(status => {
  setRunning(status.running);
  if (status.running) connectStream();
});
</script>
</body>
</html>
"""


def start_background_threads():
    threading.Thread(target=tg_daily_push_reporter, daemon=True, name="tg-daily-push-reporter").start()
    threading.Thread(target=group_push_reporter, daemon=True, name="tg-profile-card-reporter").start()
    threading.Thread(target=telegram_control_reporter, daemon=True, name="tg-private-control-reporter").start()
    resume_auto_restart_if_pending()
    threading.Thread(target=resume_crawl_if_intended, daemon=True, name="crawl-resume-on-start").start()


if __name__ == "__main__":
    init_db()
    migrate_legacy_files()
    start_background_threads()
    print("🚀 TON MAN 2.0 单文件服务启动 → http://localhost:7860")
    app.run(host="0.0.0.0", port=7860, debug=False, threaded=True)
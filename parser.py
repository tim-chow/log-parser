#coding: utf8

import sys
reload(sys)
sys.setdefaultencoding("utf8")

import time
import re
from urllib import unquote_plus, unquote
import json
from urlparse import urlparse
import traceback
import logging
import multiprocessing as mp

# !!!install: third party package!!!
from ua_parser import user_agent_parser
import MySQLdb as mdb

LOGGER = logging.getLogger()
LOCK = mp.Lock()

mysql_args = {}
mysql_args["user"] = "adviser-plat"
mysql_args["passwd"] = "adviser-plat123456"
mysql_args["db"] = "adviserdb"
mysql_args["host"] = "10.115.32.19"
mysql_args["port"] = 3306
mysql_args["charset"] = "utf8"
mysql_args["connect_timeout"] = 2

__MYSQL_CONN = None
__MYSQL_CURSOR = None

def time_used(f):
    def _inner(*a, **kw):
        start = time.time()
        try:
            result = f(*a, **kw)
        finally:
            used_time = time.time() - start
            if used_time >= 0.1:
                LOGGER.debug("func: %s uses time: %.3f ms" %
                    (f.func_name, used_time))
        return result
    return _inner

def retry(f):
    def __inner(*a, **kw):
        with LOCK:
            while True:
                try:
                    return f(*a, **kw)
                except (IOError, mdb.OperationalError):
                    global __MYSQL_CONN
                    __MYSQL_CONN = None
                    LOGGER.error("mysql error: %s" % traceback.format_exc())
                    time.sleep(0.1)
    return __inner

__CACHE = {}

@retry
def execute_sql(sql):
    if sql in __CACHE and time.time() - __CACHE[sql]["time"] <= 86400:
        return __CACHE[sql]["result"]

    global __MYSQL_CONN
    global __MYSQL_CURSOR
    if __MYSQL_CONN is None:
        __MYSQL_CONN = mdb.connect(**mysql_args)
        __MYSQL_CURSOR = __MYSQL_CONN.cursor()
    __MYSQL_CURSOR.execute(sql)

    __CACHE[sql] = {}
    __CACHE[sql]["result"] = list(__MYSQL_CURSOR)
    __CACHE[sql]["time"] = time.time()
    return __CACHE[sql]["result"]

__PC_WAP_SQL = """SELECT id,site,match_rule,match_type,match_value,page_type,page_area,page_site FROM t_pc_page_manage WHERE page_type != '' AND page_type IS NOT NULL"""

def get_pc_wap():
    field_list = "id,site,match_rule,match_type,match_value,page_type,page_area,page_site".split(",")
    result = []
    for row in execute_sql(__PC_WAP_SQL):
        result.append(dict(zip(field_list, row)))
    return result

__APP_SQL = """select site,class_name,platform,page_type,page_area,page_site from t_app_page_manage where page_type is not null and page_type != ''"""

def get_app():
    field_list = "site,class_name,platform,page_type,page_area,page_site".split(",")
    result = []
    for row in execute_sql(__PC_WAP_SQL):
        result.append(dict(zip(field_list, row)))
    return result

def get_query(url):
    result = {}
    if not url:
        return result

    url = urlparse(url)
    query = url.query.strip()
    if not query:
        return result
    for pair in query.split("&"):
        item = pair.split("=", 1)
        if len(item) != 2:
            continue
        result[item[0].strip()] = item[1].strip()
    return result

def special_deal(line):
    if line.find('"request_body":-,') != -1 or \
        line.find('"request_body":"-"') != -1:
        return None
    line = re.sub(r'"user_agent":".*?(?=Mozilla)', '"user_agent":"', line)
    return line.replace('"request_body":data=', '"request_body":')

def decode(line):
    if line.find('"request_body":%7B') != -1 or \
        line.find('"request_body":"%7B') != -1:
        return unquote_plus(line)
    return line.decode("unicode_escape")

@time_used
def normalize_one_line(one_line):
    line = special_deal(one_line)
    if line is None:
        return None, None

    line = json.loads(decode(line))
    request_body = line.get("request_body")
    if not isinstance(request_body, dict):
        return None, None

    return line, request_body

def generate_result_template(**kw):
    result = {}
    result.update(kw)
    for key in ["user_id", "ip", "phone_imei",
        "visitor_id", "session_id", "searchId",
        "url", "url_ref", "log_type", "site_id",
        "page_id", "page_intcmp", "shop_id", "mshop_id", "product_id",
        "sku_id", "group_id", "topic_id", "cmpid", "event_intcmp",
        "page_type", "page_site", "page_channel", "pre_page_type",
        "order_id", "order_type"]:
        result[key] = None

    result["time"] = -1
    result["last_session_time"] = -1
    result["session_time"] = -1
    result["info"] = {}
    return result

def get_pc_wap_page_info(key, site_id):
    for rule in get_pc_wap():
        if rule["site"] != site_id:
            continue
        if rule["match_type"] == 1:
            if key == rule["match_value"] or \
                    key == rule["match_value"] + "/":
                return rule
        elif rule["match_type"] == 2:
            if rule["match_value"] in key:
                return rule
        elif rule["match_type"] == 3:
            if key.startswith(rule["match_value"]):
                return rule
        elif rule["match_type"] == 4:
            if key.endswith(rule["match_value"]):
                return rule
        elif rule["match_type"] == 5:
            if re.search(rule["match_value"], key):
                return rule

def js_sdk_parser(one_line):
    return _js_sdk_parser(one_line)

@time_used
def _js_sdk_parser(one_line):
    line, request_body = normalize_one_line(one_line)
    if line is None:
        return None

    result = generate_result_template(flag=1)
    result["user_id"] = request_body.get("ui", {}).get("uid")
    result["ip"] = line.get("http_x_forwarded_for")
    if result["ip"]:
        result["ip"] = [ip.strip() for ip in result["ip"].split(",")][0]

    gma = request_body.get("ck", {}).get("__gma")
    if gma:
        items = gma.split(".")
        result["visitor_id"] = items[1] + "." + items[2]
    gmb = request_body.get("ck", {}).get("__gmb")
    if gmb:
        items = gmb.split(".")
        result["session_id"] =  items[2] + "." + items[3]
    gmz = request_body.get("ck", {}).get("__gmz")
    if gmz:
        gmz = unquote(gmz)
        items = gmz.split("|")
        if (len(items) > 5 and items[5] != "-"):
            result["searchId"] = items[5]

    result["site_id"] = request_body.get("ci", {}).get("tid")
    result["url_ref"] = request_body.get("pi", {}).get("dr")

    result["url"] = request_body["pi"]["dl"]
    result["info"] = get_query(result["url"])
    url = urlparse(result["url"])

    get_page_info(url, result)

    result["log_type"] = request_body.get("t")
    result["page_id"] = request_body.get("pi", {}).get("pid")
    result["page_intcmp"] = request_body.get("pi", {}).get("i")

    result["shop_id"] = request_body.get("ui", {}).get("shop_id")
    result["mshop_id"] = request_body.get("ui", {}).get("vshop_id")
    result["product_id"] = request_body.get("ui", {}).get("produce_id")
    result["sku_id"] = request_body.get("ui", {}).get("sku_id")
    result["group_id"] = request_body.get("ui", {}).get("group_id")
    result["topic_id"] = request_body.get("ui", {}).get("topic_id")
    result["cmpid"] = request_body.get("ui", {}).get("cmpid")

    result["event_intcmp"] = request_body.get("e", {}).get("i")
    result["order_id"] = request_body.get("order_id")
    result["order_type"] = request_body.get("order_system")

    try:
        result["time"] = long(time.mktime(
            time.strptime(line["@timestamp"], "%Y-%m-%dT%H:%M:%S+08:00")))
    except ValueError:
        result["time"] = long(time.mktime(
            time.strptime(line["@timestamp"], "%Y-%m-%dT%H:%M:%S 08:00")))
    last_session_time = request_body.get("pi", {}).get("lst")
    if last_session_time and last_session_time != '-':
        result['last_session_time'] = long(last_session_time)
    session_time = request_body.get("pi", {}).get("st")
    if session_time and session_time != '-':
        result["session_time"] = long(session_time)

    return json.dumps(result, encoding="utf8",
            ensure_ascii=False) + "\n"

@time_used
def get_page_info(url, result):
    protocol = url.scheme
    result["url_protocol"] = protocol
    result["url_noparam"] = protocol + "://" + url.netloc + url.path
    key = url.netloc + url.path
    matched_rule = get_pc_wap_page_info(key, result["site_id"])
    if matched_rule:
        result["page_type"] = matched_rule["page_type"]
        result["page_site"] = matched_rule["page_site"]
        result["page_channel"] = matched_rule["page_area"]

    if result["url_ref"]:
        ref_url = urlparse(result["url_ref"])
        key = ref_url.netloc + ref_url.path
        matched_rule = get_pc_wap_page_info(key, result["site_id"])
        if matched_rule:
            result["pre_page_type"] = matched_rule["page_type"]
            result["pre_page_site"] = matched_rule["page_site"]
            result["pre_page_channel"] = matched_rule["page_area"]

def replacement(m):
    request_body = m.group("request_body")

    if request_body.find("%7B%22") != -1:
        request_body = unquote_plus(request_body)
    else:
        request_body = request_body.decode("unicode_escape")
    rm = json.dumps({"request_body": json.loads(request_body)})
    rm = rm[1: len(rm)-1]
    return rm

def generate_common_result(line, request_body):
    result = generate_result_template(flag=2)
    result["ip"] = line.get("http_x_forwarded_for")
    if result["ip"]:
        result["ip"] = [ip.strip() for ip in result["ip"].split(",")][0]
    result["phone_imei"] = request_body.get("ai")
    result["visitor_id"] = request_body.get("cid")
    result["session_id"] = line.get("ssid")

    gmz = request_body.get("__gmz")
    if gmz:
        items = gmz.split("|")
        if (items[5] != "-"):
            result["searchId"] = items[5]
    result["site_id"] = request_body.get("ak")
    result["cmpid"] = request_body.get("c")

    return result

def get_app_page_info(classname, device_type, site_id):
    for rule in get_app():
        if rule["class_name"] == classname and \
                rule["platform"] == device_type and  \
                rule["site"] == site_id:
            return rule

def app_parser(one_line):
    line = special_deal(one_line)
    if line is None:
        return None

    line = re.sub(r'"request_body":"(?P<request_body>.*?)"(?=,)',
        replacement, one_line)
    line = json.loads(line)
    request_body = line["request_body"]

    e = request_body.get("e", [])
    pv = request_body.get("pv", [])
    if not e and not pv:
        result = generate_common_result(line, request_body)
        return json.dumps(result) + "\n"

    lines = []
    for new_line in e + pv:
        result = generate_common_result(line, request_body)
        lines.append(result)
        result["user_id"] = new_line.get("uid")
        result["page_id"] = new_line.get("pid")

        class_name = new_line.get("cn")
        device_type = request_body.get("dt")
        if device_type in ["1", "2"]:
            device_type = "android"
        elif device_type in ["3", "4"]:
            device_type = "ios"
        site_id = result["site_id"]

        matched_rule = get_app_page_info(class_name, device_type, site_id)
        if matched_rule:
            result["page_type"] = matched_rule["page_type"]
            result["page_site"] = matched_rule["page_site"]
            result["page_channel"] = matched_rule["page_area"]
        pre_class_name = new_line.get("lcn")
        matched_rule = get_app_page_info(pre_class_name, device_type, site_id)
        if matched_rule:
            result["pre_page_type"] = matched_rule["page_type"]
            result["pre_page_site"] = matched_rule["page_site"]
            result["pre_page_channel"] = matched_rule["page_area"]

    if not lines:
        return None
    return "\n".join([json.dumps(result) for result in lines]) + "\n"


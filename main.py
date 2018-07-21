#!/usr/bin/env python
# -*- coding: utf-8 -*
# 上面一行用来支持中文注释

# 添加引用库
from flask import Flask, request, jsonify, abort
import os
import hashlib
import sys
import json
import argparse
import time
import datetime
import calendar
import elasticsearch
from functools import wraps

# 创建全局变量k  
global g_flask_app
g_flask_app = Flask(__name__)

g_version = "1.0.5"


# 创建ESClient对象
class ElasticSearchClient(object):
    @staticmethod
    def get_cli():
        es_servers = [{
            "host": args.es_host,
            "port": args.es_port
        }]
        try:
            es_client = elasticsearch.Elasticsearch(hosts=es_servers,
                                                    sniff_on_start=True,
                                                    sniff_on_connection_fail=True,
                                                    sniffer_timeout=600)
        except Exception, err:
            return abort(jsonify({'code': -5, 'msg': 'ES server cant connect', 'data': None}))
        return es_client


# 将本地库目录添加到系统路径中
def init_import_path():
    if __package__ is None:
        import sys
        from os import path
        sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))


# 初始化日志系统
def init_logger():
    from logbook import Logger, StreamHandler
    StreamHandler(sys.stdout).push_application()
    # 创建全局变量
    global g_log
    g_log = Logger('FKLog')


# 初始化Flask
# Ref: http://www.pythondoc.com/flask/config.html
def init_flask(config_file_name):
    # 加载配置文件
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(current_dir, 'Config', config_file_name)
    g_flask_app.config.from_json(config_path, silent=True)
    g_log.info(g_flask_app.config)


# 执行主循环
def start_listening():
    if args.use_https:
        g_log.error("警告：当前暂不支持HTTPS模式（需要引入pyOpenSSL库太麻烦）")
        g_log.info("Https://" + args.api_host + ":" + str(args.api_port))
        g_log.info(args.debug)
        g_flask_app.run(host=args.api_host, port=args.api_port, debug=args.debug, ssl_context='adhoc')
    else:
        g_log.info(args.debug)
        g_log.info("Http://" + args.api_host + ":" + str(args.api_port))
        g_flask_app.run(host=args.api_host, port=args.api_port, debug=args.debug)


# API调用频率限制（seconds单位是秒）
def runFrequencyLimit(seconds=0.1):
    def call_func(fn):
        cache = {}

        @wraps(fn)
        def wrapper(*params, **paramMap):
            if fn.__name__ in cache:
                last_time = cache[fn.__name__]
                if time.time() - last_time > seconds:
                    ret = fn(*params, **paramMap)
                    cache[fn.__name__] = time.time()
                    return ret
                else:
                    return abort(jsonify({'code': -2, 'msg': 'access too fast', 'data': None}))
            else:
                ret = fn(*params, **paramMap)
                cache[fn.__name__] = time.time()
                return ret

        return wrapper

    return call_func


# 一些基本的页面处理
@g_flask_app.route('/')
def index():
    return "Hello World"


@g_flask_app.route('/favicon.ico')
def favicon():
    return ''


@g_flask_app.route('/version')
def version():
    return g_version


@g_flask_app.errorhandler(403)
def err_403(error):
    g_log.info('403 Resource forbidden: ' + request.path + ' , client ip : ' + request.remote_addr)
    return '403 forbidden'  # abort(jsonify({'code': -6, 'msg': '403 forbidden', 'data': None}))


@g_flask_app.errorhandler(404)
def err_404(error):
    g_log.info('404 Page not found: ' + request.path + ' , client ip : ' + request.remote_addr)
    return '404 url not right'  # abort(jsonify({'code': -7, 'msg': '404 url not right', 'data': None}))


@g_flask_app.errorhandler(500)
def err_500(error):
    g_log.info('500 error found: ' + request.path + ' , client ip : ' + request.remote_addr)
    return '500 server error'  # abort(jsonify({'code': -8, 'msg': '500 server error', 'data': None}))


# 计算MD5列表
def cal_md5_list(valid_platform_list):
    ret_list = {}
    for key, value in valid_platform_list.items():
        m = hashlib.md5()
        m.update(value + "FreeKnight")
        ret_list[value] = m.hexdigest()
    return ret_list


# 收到消息时的处理：检查产品Key是否正确
@g_flask_app.before_request
def before_request():
    g_log.info('Remote ip : ' + request.remote_addr + ' access uri : ' + request.url)
    result_data = {'code': 0, 'msg': 'success', 'data': None}

    valid_platform_list = g_flask_app.config['VALID_PLATFORM_CODES']
    valid_auth_list = cal_md5_list(valid_platform_list)

    # for key,value in valid_auth_list.items():
    #   print valid_auth_list

    if args.auth_check == False:
        return

    while True:
        auth_code = request.headers.get('auth_code')
        if auth_code is None:
            result_data['code'] = -1
            result_data['msg'] = "auth code not found !"
            break

        auth_code = auth_code.lower()
        if auth_code not in valid_auth_list.values():
            result_data['code'] = -1
            result_data['msg'] = "invalid auth code !"
            break
        else:
            for key, value in valid_auth_list.items():
                if value == auth_code:
                    auth_code_name = key
                    break

            g_log.info(auth_code_name + " comes to request.")
            break
        break

    if result_data['code'] == -1:
        abort(jsonify(result_data))


# 组装后的处理：添加点小标示
@g_flask_app.after_request
def after_request(response):
    response.headers['Server'] = 'FKAPIServer'
    return response


# Unix格式化时间
def unix_timestamp(dt):
    return (time.mktime(dt.timetuple()) * 1000)


# ISO格式化时间     "2018-04-04T15:08:14.682000+08:00"
def iso_8601_format(dt):
    if dt is None:
        return ""

    fmt_datetime = dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
    fmt_timezone = "+08:00"
    """
    tz = dt.utcoffset()
    if tz is None:
        fmt_timezone = "+00:00"
    else:
        fmt_timezone = str.format('{0:+06.2f}', float(tz.total_seconds() / 3600))
    """
    return fmt_datetime + fmt_timezone


# 获取单日的起始时间和结束时间
def get_start_end_time_of_day_dt(dt):
    startTime = datetime.datetime.combine(dt, datetime.time.min)
    endTime = datetime.datetime.combine(dt, datetime.time.max)
    return startTime, endTime


# 获取单日的起始时间和结束时间
def get_start_end_time_of_day(year, month, day):
    if year:
        year = int(year)
    if month:
        month = int(month)
    if day:
        day = int(day)
    dt = datetime.date(year=year, month=month, day=day)
    return get_start_end_time_of_day_dt(dt)


# 检查年份是否合理
def check_year(year):
    if isinstance(year, int) != True:
        abort(jsonify({'code': -3, 'msg': 'Param [year] is not a int.', 'data': None}))
    if year > 2100 or year < 2000:
        abort(jsonify({'code': -3, 'msg': 'Param [year] is out of range, must in [2000,2100].', 'data': None}))


# 检查月份是否合理
def check_month(month):
    if isinstance(month, int) != True:
        abort(jsonify({'code': -3, 'msg': 'Param [month] is not a int.', 'data': None}))
    if month > 12 or month < 1:
        abort(jsonify({'code': -3, 'msg': 'Param [month] is out of range, must in [1,12].', 'data': None}))


# 检查日期是否合理
def check_day(day):
    if isinstance(day, int) != True:
        abort(jsonify({'code': -3, 'msg': 'Param [day] is not a int.', 'data': None}))
    if day > 31 or day < 1:
        abort(jsonify({'code': -3, 'msg': 'Param [day] is out of range, must in [1,31].', 'data': None}))


# 获取指定天的本周第一天
def first_day_of_week(dt):
    return dt - datetime.timedelta(days=dt.weekday())


# 获取指定天的本周最后一天
def last_day_of_week(dt):
    return dt + datetime.timedelta(days=6 - dt.weekday())


# 获取指定天的本月第一天
def first_day_of_month(dt):
    year = dt.year
    month = dt.month
    return datetime.date(year=year, month=month, day=1)


# 获取指定天的本月最后一天
def last_day_of_month(dt):
    year = dt.year
    month = dt.month
    firstDayWeekDay, monthRange = calendar.monthrange(year, month)
    return datetime.date(year=year, month=month, day=monthRange)


# 获取指定天的本年第一天
def first_day_of_year(dt):
    year = dt.year
    return datetime.date(year=year, month=1, day=1)


# 获取指定天的本年最后一天
def last_day_of_year(dt):
    year = dt.year
    return datetime.date(year=year, month=12, day=31)


# 获取指定时间段的起始时间和最终时间
def get_start_last_time(startDay, endDay, type):
    try:
        isDay = (type == "day")
        isWeek = (type == "week")
        isMonth = (type == "month")
        isYear = (type == "year")

        startDayDT = datetime.datetime.strptime(startDay, "%Y-%m-%d")
        endDayDT = datetime.datetime.strptime(endDay, "%Y-%m-%d")
        if isDay:
            realStartDay = startDayDT
            realEndDay = endDayDT
        elif isWeek:
            realStartDay = first_day_of_week(startDayDT)
            realEndDay = last_day_of_week(endDayDT)
        elif isMonth:
            realStartDay = first_day_of_month(startDayDT)
            realEndDay = last_day_of_month(endDayDT)
        elif isYear:
            realStartDay = first_day_of_year(startDayDT)
            realEndDay = last_day_of_year(endDayDT)

        startDayBeginTime, tmp = get_start_end_time_of_day_dt(realStartDay)
        tmp, endDayLastTime = get_start_end_time_of_day_dt(realEndDay)
        # startTime = iso_8601_format(startDayBeginTime)
        # endTime = iso_8601_format(endDayLastTime)
    except Exception, err:
        return abort(jsonify({'code': -4, 'msg': 'error time format', 'data': None}))

    return startDayBeginTime, endDayLastTime


# 获取时间列表
def get_time_list(startTime, endTime, type):
    try:
        isDay = (type == "day")
        isWeek = (type == "week")
        isMonth = (type == "month")
        isYear = (type == "year")

        timeList = []
        tmpStart = startTime

        if isDay:
            while True:
                tmp, tmpEnd = get_start_end_time_of_day_dt(tmpStart)
                if tmpEnd > endTime:
                    break
                else:
                    timeList.append(tmpStart)
                    timeList.append(tmpEnd)
                    tmpStart, tmp = get_start_end_time_of_day_dt(tmpEnd + datetime.timedelta(days=1))
        elif isWeek:
            while True:
                tmp, tmpEnd = get_start_end_time_of_day_dt(last_day_of_week(tmpStart))
                if tmpEnd > endTime:
                    break
                else:
                    timeList.append(tmpStart)
                    timeList.append(tmpEnd)
                    tmpStart, tmp = get_start_end_time_of_day_dt(tmpEnd + datetime.timedelta(days=1))
        elif isMonth:
            while True:
                tmp, tmpEnd = get_start_end_time_of_day_dt(last_day_of_month(tmpStart))
                if tmpEnd > endTime:
                    break
                else:
                    timeList.append(tmpStart)
                    timeList.append(tmpEnd)
                    tmpStart, tmp = get_start_end_time_of_day_dt(tmpEnd + datetime.timedelta(days=1))
        elif isYear:
            while True:
                tmp, tmpEnd = get_start_end_time_of_day_dt(last_day_of_year(tmpStart))
                if tmpEnd > endTime:
                    break
                else:
                    timeList.append(tmpStart)
                    timeList.append(tmpEnd)
                    tmpStart, tmp = get_start_end_time_of_day_dt(tmpEnd + datetime.timedelta(days=1))
        else:
            return abort(jsonify({'code': -4, 'msg': 'error time format', 'data': None}))

    except Exception, err:
        print(err)
        return abort(jsonify({'code': -4, 'msg': 'error time format', 'data': None}))

    return timeList


# 获取指定时间内的年月列表
def get_month_list(startTime, endTime):
    timeList = []
    timeYearMonthList = []
    while True:
        tmpStart, noUse = get_start_end_time_of_day_dt(first_day_of_month(startTime))
        if tmpStart > endTime:
            break
        else:
            timeList.append(tmpStart)
            startTime, noUse = get_start_end_time_of_day_dt(datetime.datetime(
                tmpStart.year + (tmpStart.month / 12), ((tmpStart.month % 12) + 1), 1))

    for i in timeList:
        timeYearMonthList.append(i.strftime('%Y.%m'))

    return timeYearMonthList


# 检查Index是否存在并进行筛选
def filter_valid_index(es, es_index_prefix, monthList):
    retArr = []
    for month in monthList:
        tmpIndex = es_index_prefix + "-" + month
        if es.indices.exists(index=tmpIndex):
            retArr.append(tmpIndex)
    return retArr


# 转换python list-> Json string
def list_to_str(pythonList):
    if isinstance(pythonList, list):
        ret = json.dumps(pythonList)  # [1:-1].replace("\\\"", "\"")
        return ret
    elif isinstance(pythonList, dict):
        ret = json.dumps(pythonList)  # [1:-1].replace("\\\"", "\"")
        return ret
    else:
        return ""


# 设置索引别名脚本组装
def get_update_aliases_script(aliase, es_index_prefix, monthList):
    retJson = {}
    actions = []
    remove_index = {"remove": {"index": "*", "alias": aliase}}
    actions.append(remove_index)
    base_index = {"add": {"index": es_index_prefix, "alias": aliase}}
    actions.append(base_index)
    for month in monthList:
        tmp = {"add": {"index": es_index_prefix + "-" + month, "alias": aliase}}
        actions.append(tmp)
    retJson['actions'] = actions
    return retJson
    # {
    #    "actions": [
    #        { "remove_index": { "index": "test" } }
    #        { "add":    { "index": "tweets_2", "alias": "tweets_search" }},
    #        { "add":    { "index": "tweets_2", "alias": "tweets_index"  }}
    #    ]
    # }


# 获取索引列表
def get_indices_by_month(es, es_index_prefix, monthList):
    retArr = []
    retStr = ""
    # retArr.append(es_index_prefix)
    # for month in monthList:
    #    retArr.append( es_index_prefix + "-" + month)
    retArr = filter_valid_index(es, es_index_prefix, monthList)
    index = 0
    while index < len(retArr):
        retStr += retArr[index]
        retStr += ","
        index += 1
    return retStr[0:-1]


# 查询UV/PV接口脚本组装
def get_uvpv_script(es, tagIdList, pid, timeList, startTime, endTime):
    if is_es_version_5(es):
        user_id_keyword = "session_id"
    else:
        user_id_keyword = "session_id.keyword"

    queryJson = {"bool": {
        "must": [
            {
            "range":
                {
                    "event_time":
                        {
                            "gte": iso_8601_format(startTime),
                            "lte": iso_8601_format(endTime)
                        }
                }
            },
            {"match": { "product_id": pid }}
        ]
    }
    }
    # print("queryJson")
    # print(queryJson)
    aggsJson = {
        "total_uv": {
            "cardinality": {
                "field": user_id_keyword
            }
        }
    }

    for tag in tagIdList:
        tagAggsJson = {
            tag + "_uv_total": {
                "cardinality": {
                    "field": user_id_keyword
                }
            }
        }
        index = 0
        while index < len(timeList):
            per_time_section_pv_index = tag + "_pv_" + str(index / 2)
            per_time_section_uv_index = tag + "_uv_" + str(index / 2)
            per_time_section_json = {
                "range": {
                    "field": "event_time",
                    "ranges":
                        {"from": iso_8601_format(timeList[index]),
                         "to": iso_8601_format(timeList[index + 1])}
                },

                "aggs": {
                    per_time_section_uv_index: {
                        "cardinality": {
                            "field": user_id_keyword
                        }
                    }
                }
            }
            # print(per_time_section_pv_index)
            # print(per_time_section_json)
            tagAggsJson[per_time_section_pv_index] = per_time_section_json
            index += 2
        filterJson = {
            "bool": {
                "must": [
                    {"match":
                         {"tag_id": tag}
                     }
                ]
            }
        }
        aggsJson[tag + "_pv_total"] = {
            "filter": filterJson,
            "aggs": tagAggsJson
        }

    for tag in tagIdList:
        tagAggsJson_total = {
            "uv_total": {
                "cardinality": {
                    "field": user_id_keyword
                }
            }
        }
        index = 0
        while index < len(timeList):
            per_time_section_pv_total_index = "pv_" + str(index / 2)
            per_time_section_uv_total_index = "uv_" + str(index / 2)
            per_time_section_total_json = {
                "range": {
                    "field": "event_time",
                    "ranges":
                        {"from": iso_8601_format(timeList[index]),
                         "to": iso_8601_format(timeList[index + 1])}
                },

                "aggs": {
                    per_time_section_uv_total_index: {
                        "cardinality": {
                            "field": user_id_keyword
                        }
                    }
                }
            }
            # print(per_time_section_pv_index)
            # print(per_time_section_json)
            tagAggsJson_total[per_time_section_pv_total_index] = per_time_section_total_json
            index += 2
        filterJson_total = {"exists":
            {
                "field": "tag_id"
            }
        }
        aggsJson["pv_total"] = {
            "filter": filterJson_total,
            "aggs": tagAggsJson_total
        }
    # print("aggsJson")
    # print(aggsJson)
    total = {
        "size": 5,
        "query": queryJson,
        "aggs": aggsJson
    }

    # total = {
    #
    #    "aggs": {
    #        "total_uv": {
    #            "cardinality": {
    #                "field": "tag_id.keyword"
    #            }
    #        }
    #    },
    #    "size": 1
    #
    # }
    # print("total")
    # print(total)
    return total


# 解析UV,PV查询结果
def parser_res_uvpv(res, tagIdList, timeList):
    # print res
    #printTotalPV = 0
    #printTotalUV = 0
    #printTotalUVRatio = 0
    #printTotalPVRatio = 0
    result_data = {}
    result_data['series'] = []
    for tag in tagIdList:
        tagNode = {}
        tagNode["tag_id"] = tag
        tagPVTotal = res['aggregations'][tag + "_pv_total"]["doc_count"]
        tagNode['total_PV'] = tagPVTotal
        tagUVTotal = res['aggregations'][tag + "_pv_total"][tag + "_uv_total"]['value']
        tagNode['total_UV'] = tagUVTotal
        tagTimeList = []
        for index in range(0, len(timeList) / 2):
            tagTimeNode = {}
            tagPVtime = res['aggregations'][tag + "_pv_total"][tag + "_pv_" + str(index)]["buckets"][0]["doc_count"]
            tagUVtime = \
            res['aggregations'][tag + "_pv_total"][tag + "_pv_" + str(index)]["buckets"][0][tag + "_uv_" + str(index)][
                'value']
            totalPVTime = res['aggregations']["pv_total"]["pv_" + str(index)]["buckets"][0]["doc_count"]
            totalUVTime = res['aggregations']["pv_total"]["pv_" + str(index)]["buckets"][0]["uv_" + str(index)]['value']
            timeStart = res['aggregations'][tag + "_pv_total"][tag + "_pv_" + str(index)]["buckets"][0][
                            "from_as_string"][0:-14]
            timeEnd = res['aggregations'][tag + "_pv_total"][tag + "_pv_" + str(index)]["buckets"][0]["to_as_string"][
                      0:-14]
            tagTimeNode["PV"] = tagPVtime
            tagTimeNode["UV"] = tagUVtime
            if totalUVTime != 0:
                tagTimeNode["UV_ratio"] = tagUVtime * 100.0 / totalUVTime
            else:
                tagTimeNode["UV_ratio"] = 0
            if totalPVTime != 0:
                tagTimeNode["PV_ratio"] = tagPVtime * 100.0 / totalPVTime
            else:
                tagTimeNode["PV_ratio"] = 0
            tagTimeNode["start_date"] = timeStart
            tagTimeNode["end_date"] = timeEnd
            #printTotalPV += tagPVtime
            #printTotalUV += tagUVtime
            #printTotalPVRatio += (tagPVtime * 100 / totalPVTime)
            #printTotalUVRatio += (tagUVtime * 100 / totalUVTime)
            #g_log.info("[" + timeStart + " - " + timeEnd + "] " + tag + "   " + str(printTotalPV) + "/" + str(totalPVTime) +
            #           " tagPVtime:" + str(tagPVtime) + " " + tagTimeNode["PV_ratio"] + " PV_ratio:" + str(printTotalPVRatio))
            #g_log.info("[" + timeStart + " - " + timeEnd + "] " + tag + "   " + str(printTotalUV) + "/" + str(totalUVTime) +
            #           " tagUVtime:" + str(tagUVtime) + " " + tagTimeNode["UV_ratio"] + " UV_ratio:" + str(printTotalUVRatio))
            tagTimeList.append(tagTimeNode)
        tagNode["detail"] = tagTimeList
        result_data['series'].append(tagNode)

    result_data['all_PV'] = res['hits']['total']
    result_data['all_UV'] = res['aggregations']['total_uv']

    return result_data


# 查询渠道下载接口脚本组装
def get_pal_script(palCodeList, pid, timeList, startTime, endTime):
    queryJson = {
        "bool": {
            "must": [{
                "range":
                    {
                        "event_time":
                            {
                                "gte": iso_8601_format(startTime),
                                "lte": iso_8601_format(endTime)
                            }
                    }
            },
            {"match": { "product_id": pid }}
            ]
        }
    }
    # print("queryJson")
    # print(queryJson)
    aggsJson = {}

    for palCode in palCodeList:
        tagAggsJson_install_Android = {}
        index = 0
        while index < len(timeList):
            per_time_section_index = palCode + "_install_android_" + str(index / 2)
            per_time_section_json = {
                "range": {
                    "field": "event_time",
                    "ranges":
                        {"from": iso_8601_format(timeList[index]),
                         "to": iso_8601_format(timeList[index + 1])}
                }
            }
            tagAggsJson_install_Android[per_time_section_index] = per_time_section_json
            index += 2
        filterJson_install_Android = {
            "bool": {
                "must": [
                    {"match":
                         {"palcode": palCode}
                     },
                    {"match":
                         {"tag_id": "c_Initial"}
                     },
                    {"match":
                         {"os_type": "Android"}
                     }
                ]
            }
        }
        aggsJson[palCode + "_install_android_total"] = {
            "filter": filterJson_install_Android,
            "aggs": tagAggsJson_install_Android
        }

    for palCode in palCodeList:
        tagAggsJson_install_iOS = {}
        index = 0
        while index < len(timeList):
            per_time_section_index = palCode + "_install_ios_" + str(index / 2)
            per_time_section_json = {
                "range": {
                    "field": "event_time",
                    "ranges":
                        {"from": iso_8601_format(timeList[index]),
                         "to": iso_8601_format(timeList[index + 1])}
                }
            }
            tagAggsJson_install_iOS[per_time_section_index] = per_time_section_json
            index += 2
        filterJson_install_iOS = {
            "bool": {
                "must": [
                    {"match":
                         {"palcode": palCode}
                     },
                    {"match":
                         {"tag_id": "c_Initial"}
                     },
                    {"match":
                         {"os_type": "iOS"}
                     }
                ]
            }
        }
        aggsJson[palCode + "_install_ios_total"] = {
            "filter": filterJson_install_iOS,
            "aggs": tagAggsJson_install_iOS
        }

    for palCode in palCodeList:
        tagAggsJson_install_all = {}
        index = 0
        while index < len(timeList):
            per_time_section_index = palCode + "_install_all_" + str(index / 2)
            per_time_section_json = {
                "range": {
                    "field": "event_time",
                    "ranges":
                        {"from": iso_8601_format(timeList[index]),
                         "to": iso_8601_format(timeList[index + 1])}
                }
            }
            tagAggsJson_install_all[per_time_section_index] = per_time_section_json
            index += 2
        filterJson_install_all = {
            "bool": {
                "must": [
                    {"match":
                         {"palcode": palCode}
                     },
                    {"match":
                         {"tag_id": "c_Initial"}
                     }
                ]
            }
        }
        aggsJson[palCode + "_install_all_total"] = {
            "filter": filterJson_install_all,
            "aggs": tagAggsJson_install_all
        }

    for palCode in palCodeList:
        tagAggsJson_download_android = {}
        index = 0
        while index < len(timeList):
            per_time_section_index = palCode + "_download_android_" + str(index / 2)
            per_time_section_json = {
                "range": {
                    "field": "event_time",
                    "ranges":
                        {"from": iso_8601_format(timeList[index]),
                         "to": iso_8601_format(timeList[index + 1])}
                }
            }
            tagAggsJson_download_android[per_time_section_index] = per_time_section_json
            index += 2
        filterJson_download_android = {
            "bool": {
                "must": [
                    {"match":
                         {"palcode": palCode}
                     },
                    {"match":
                         {"tag_id": "download_android"}
                     }
                ]
            }
        }
        aggsJson[palCode + "_download_android_total"] = {
            "filter": filterJson_download_android,
            "aggs": tagAggsJson_download_android
        }

    for palCode in palCodeList:
        tagAggsJson_download_iOS = {}
        index = 0
        while index < len(timeList):
            per_time_section_index = palCode + "_download_ios_" + str(index / 2)
            per_time_section_json = {
                "range": {
                    "field": "event_time",
                    "ranges":
                        {"from": iso_8601_format(timeList[index]),
                         "to": iso_8601_format(timeList[index + 1])}
                }
            }
            tagAggsJson_download_iOS[per_time_section_index] = per_time_section_json
            index += 2
        filterJson_download_iOS = {
            "bool": {
                "must": [
                    {"match":
                         {"palcode": palCode}
                     },
                    {"match":
                         {"tag_id": "download_ios"}
                     }
                ]
            }
        }
        aggsJson[palCode + "_download_ios_total"] = {
            "filter": filterJson_download_iOS,
            "aggs": tagAggsJson_download_iOS
        }
    # print("aggsJson")
    # print(aggsJson)
    total = {
        "size": 0,
        "query": queryJson,
        "aggs": aggsJson
    }
    # print("total")
    # print(total)-`
    return total


# 解析渠道下载查询结果
def parser_res_pal(res, palCodeList, timeList):
    result_data = {}
    result_data['series'] = []
    for palCode in palCodeList:
        palNode = {}
        palNode["palcode"] = palCode

        palInstallAndroidTotal = res['aggregations'][palCode + "_install_android_total"]["doc_count"]
        palNode['install_android_total'] = palInstallAndroidTotal
        palInstalliOSTotal = res['aggregations'][palCode + "_install_ios_total"]["doc_count"]
        palNode['install_ios_total'] = palInstalliOSTotal
        palInstallAllTotal = res['aggregations'][palCode + "_install_all_total"]["doc_count"]
        palNode['install_all_total'] = palInstallAllTotal
        palDownloadAndroidTotal = res['aggregations'][palCode + "_download_android_total"]["doc_count"]
        palNode['download_android_total'] = palDownloadAndroidTotal
        palDownloadiOSTotal = res['aggregations'][palCode + "_download_ios_total"]["doc_count"]
        palNode['download_ios_total'] = palDownloadiOSTotal

        palTimeList = []
        for index in range(0, len(timeList) / 2):
            palTimeNode = {}
            palInstallAndroid = \
            res['aggregations'][palCode + "_install_android_total"][palCode + "_install_android_" + str(index)][
                "buckets"][0]["doc_count"]
            palInstalliOS = \
            res['aggregations'][palCode + "_install_ios_total"][palCode + "_install_ios_" + str(index)]["buckets"][0][
                "doc_count"]
            palInstallAll = \
            res['aggregations'][palCode + "_install_all_total"][palCode + "_install_all_" + str(index)]["buckets"][0][
                "doc_count"]
            palDownloadAndroid = \
            res['aggregations'][palCode + "_download_android_total"][palCode + "_download_android_" + str(index)][
                "buckets"][0]["doc_count"]
            palDownloadiOS = \
            res['aggregations'][palCode + "_download_ios_total"][palCode + "_download_ios_" + str(index)]["buckets"][0][
                "doc_count"]

            timeStart = \
            res['aggregations'][palCode + "_install_android_total"][palCode + "_install_android_" + str(index)][
                "buckets"][0]["from_as_string"][0:-14]
            timeEnd = \
            res['aggregations'][palCode + "_install_android_total"][palCode + "_install_android_" + str(index)][
                "buckets"][0]["to_as_string"][0:-14]

            palTimeNode["install_android"] = palInstallAndroid
            palTimeNode["install_ios"] = palInstalliOS
            palTimeNode["install_all"] = palInstallAll
            palTimeNode["download_android"] = palDownloadAndroid
            palTimeNode["download_ios"] = palDownloadiOS
            palTimeNode["start_date"] = timeStart
            palTimeNode["end_date"] = timeEnd
            palTimeList.append(palTimeNode)
        palNode["detail"] = palTimeList
        result_data['series'].append(palNode)

    return result_data


# 检查ES版本
def is_es_version_5(es):
    try:
        mainVer = es.info()["version"]["number"].split('.')[0]
        if mainVer == str(5):
            return True
        else:
            return False
    except:
        return True
    return False


# 查询UV/PV接口
# http://10.71.12.113:5000/pvuv/pid=b01&startday=2018-03-08&endday=2018-03-25
# http://10.71.12.113:5000/pvuv/pid=b01&startday=2018-03-08&endday=2018-03-25&type=day
# http://10.71.12.113:5000/pvuv/pid=b01&tagid=p02&startday=2018-04-01&endday=2018-04-30&type=week
# http://10.71.12.113:5000/pvuv/pid=b01&tagid=p02|p03&startday=2016-02-01&endday=2018-04-30&type=year
@runFrequencyLimit()
@g_flask_app.route('/pvuv/pid=<string:pid>&startday=<string:startDay>&endday=<string:endDay>')
@g_flask_app.route('/pvuv/pid=<string:pid>&startday=<string:startDay>&endday=<string:endDay>&type=<string:type>')
@g_flask_app.route('/pvuv/pid=<string:pid>&tagid=<string:tagId>&startday=<string:startDay>&endday=<string:endDay>')
@g_flask_app.route('/pvuv/pid=<string:pid>&tagid=<string:tagId>&startday=<string:startDay>&endday=<string:endDay>&type=<string:type>')
@g_flask_app.route(
    '/pvuv/pid=<string:pid>&tagid=<string:tagId>&startday=<string:startDay>&endday=<string:endDay>&type=<string:type>&pageindex=<int:pageIndex>&perpagesize=<int:perPageSize>')
def request_uvpv(pid, startDay, endDay, type="day", tagId="all", pageIndex=1, perPageSize=50):
    g_log.info("Request UV/PV : pid = " + pid + " ,tagId = " + tagId + " ,startDay = " + startDay + " ,endDay = " + endDay
               + " ,type = " + type + " ,pageIndex = " + str(pageIndex) + " ,perPageSize = " + str(perPageSize))
    result_json = {'code': 0, 'msg': 'success', 'data': None}

    es = ElasticSearchClient.get_cli()

    oneTagId = ""
    tagIdList = []
    if tagId == "all":
        all_tag_id_dic = g_flask_app.config['TAG_ID_LIST']
        for v in all_tag_id_dic.values():
            oneTagId += v
            oneTagId += '|'
        tagId = oneTagId[0:-1]
        tagIdList = tagId.split('|')
    else:
        tagIDIndexList = tagId.split('|')
        all_tag_id_dic = g_flask_app.config['TAG_ID_LIST']
        for tagIDIndex in tagIDIndexList:
            for key, value in all_tag_id_dic.items():
                if tagIDIndex == key:
                    tagIdList.append(value)
                    break
    print "=====tag id list====="
    for e in tagIdList:
        print e
    print "=====tag id list====="

    startTime, endTime = get_start_last_time(startDay, endDay, type)
    # print "=====time range====="
    # print startTime
    # print endTime
    # print "=====time range====="

    timeList = get_time_list(startTime, endTime, type)
    # print "=====time list====="
    # for e in timeList:
    #    print e
    # print "=====time list====="

    monthList = get_month_list(startTime, endTime)
    # print "=====month list====="
    # for e in monthList:
    #    print e
    # print "=====month list====="

    # 组装查询脚本
    script = get_uvpv_script(es, tagIdList, pid, timeList, startTime, endTime)
    #print "=====es script====="
    #print(list_to_str(script))
    #print "=====es script====="

    # 放弃索引别名的方式
    # updateScript = get_update_aliases_script("DA_SHENG_INDEX_ALIASES", es_index_prefix, monthList)
    # print "=====aliases script====="
    # print(list_to_str(updateScript))
    # print "=====aliases script====="
    # ElasticSearchClient.get_cli().indices.update_aliases(updateScript)
    # res = ElasticSearchClient.get_cli().search(index="DA_SHENG_INDEX_ALIASES", body=script)

    # 组装查询索引
    es_index_prefix = args.es_index_prefix
    indices = get_indices_by_month(es, es_index_prefix, monthList)
    #print "=====sreach indices====="
    #print(indices)
    #print "=====sreach indices====="

    # 执行脚本
    try:
        res = es.search(index=indices, body=script)
        result_json['data'] = parser_res_uvpv(res, tagIdList, timeList)
    except Exception, err:
        print(err)
        return abort(jsonify({'code': -7, 'msg': 'Es search failed.', 'data': None}))

    return jsonify(result_json)


# 查询渠道信息接口
# http://10.71.12.113:5000/pal/pid=a01&startday=2018-03-08&endday=2018-03-25
# http://10.71.12.113:5000/pal/pid=a01&startday=2018-03-08&endday=2018-03-25&type=day
# http://10.71.12.113:5000/pal/pid=a01&palcode=p02&startday=2018-04-01&endday=2018-04-30&type=week
# http://10.71.12.113:5000/pal/pid=a01&palcode=p02|p03|p04&startday=2016-02-01&endday=2018-04-30&type=year
@runFrequencyLimit()
@g_flask_app.route('/pal/pid=<string:pid>&startday=<string:startDay>&endday=<string:endDay>')
@g_flask_app.route('/pal/pid=<string:pid>&startday=<string:startDay>&endday=<string:endDay>&type=<string:type>')
@g_flask_app.route('/pal/pid=<string:pid>&palcode=<string:palcode>&startday=<string:startDay>&endday=<string:endDay>')
@g_flask_app.route('/pal/pid=<string:pid>&palcode=<string:palcode>&startday=<string:startDay>&endday=<string:endDay>&type=<string:type>')
@g_flask_app.route(
    '/pal/pid=<string:pid>&palcode=<string:palcode>&startday=<string:startDay>&endday=<string:endDay>&type=<string:type>&pageindex=<int:pageIndex>&perpagesize=<int:perPageSize>')
def request_channel(pid, startDay, endDay, type="day", palcode="all", pageIndex=1, perPageSize=50):
    g_log.info("Request channel : pid = " + pid + " ,palcode = " + palcode + " ,startDay = " + startDay + " ,endDay = " + endDay
               + " ,type = " + type + " ,pageIndex = " + str(pageIndex) + " ,perPageSize = " + str(perPageSize))
    result_json = {'code': 0, 'msg': 'success', 'data': None}

    es = ElasticSearchClient.get_cli()
    print es.info()

    onePalcode = ""
    if palcode == "all":
        all_pal_code_dic = g_flask_app.config['PAL_CODE_LIST']
        for v in all_pal_code_dic.values():
            onePalcode += str(v)
            onePalcode += '|'
        palcode = onePalcode[0:-1]
    palcodeList = palcode.split('|')
    # print "=====pal code list====="
    # for e in palcodeList:
    #    print e
    # print "=====pal code list====="

    startTime, endTime = get_start_last_time(startDay, endDay, type)
    # print "=====time range====="
    # print startTime
    # print endTime
    # print "=====time range====="

    timeList = get_time_list(startTime, endTime, type)
    # print "=====time list====="
    # for e in timeList:
    #    print e
    # print "=====time list====="

    monthList = get_month_list(startTime, endTime)
    # print "=====month list====="
    # for e in monthList:
    #    print e
    # print "=====month list====="

    # 组装查询脚本
    script = get_pal_script(palcodeList, pid, timeList, startTime, endTime)
    #print "=====es script====="
    #print(list_to_str(script))
    #print "=====es script====="

    # 组装查询索引
    es_index_prefix = args.es_index_prefix
    indices = get_indices_by_month(es, es_index_prefix, monthList)
    #print "=====sreach indices====="
    #print(indices)
    #print "=====sreach indices====="

    # 执行脚本
    try:
        res = es.search(index=indices, body=script)
        result_json['data'] = parser_res_pal(res, palcodeList, timeList)
    except Exception, err:
        print(err)
        return abort(jsonify({'code': -7, 'msg': 'Es search failed.', 'data': None}))

    return jsonify(result_json)


@g_flask_app.route('/getTagidList.html')
def request_get_tagid_list():
    all_pal_code_dic = g_flask_app.config['TAG_ID_LIST']
    json = list_to_str(all_pal_code_dic)
    return json


#@g_flask_app.route('/getPalcodeList.html')
@g_flask_app.route('/getPalcodeList/pid=<string:pid>')
def request_get_palcode_list(pid):
    g_log.info("Request get palcode list: pid = " + pid)
    all_pal_code_dic = g_flask_app.config['PAL_CODE_LIST']
    pal_code_by_pid = all_pal_code_dic[pid]
    json = list_to_str(pal_code_by_pid)
    return json


# 入口Main函数
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='FK API Server.')
    parser.add_argument('--api-host',
                        default='0.0.0.0',
                        help='The host/IP to be used by the API Server, defaults to \"0.0.0.0\" all interfaces.')
    parser.add_argument('--api-port',
                        type=int,
                        default=8080,
                        help='The port number to be used by the API Server, defaults to 8080.')
    parser.add_argument('--es-host',
                        default='127.0.0.1',
                        help='The host/IP for connecting to the Elasticsearch cluster, defaults to \"127.0.0.1\".')
    parser.add_argument('--es-port',
                        type=int,
                        default='9200',
                        help='The port number for connecting to the Elasticsearch cluster, defaults to 9200.')
    parser.add_argument('--es-index-prefix',
                        default='',
                        help='The prefix of index name in Elasticsearch to be used, defaults to \"da_shen_test\".')
    parser.add_argument('--use-https',
                        action='store_true',
                        help='Indicating if the server should be started with HTTPS support, defaults to false.')
    parser.add_argument('--auth-check',
                        action='store_true',
                        help='Indicating if the server should check authentication code.')
    parser.add_argument('--debug',
                        action='store_true',
                        help='Indicating if the server should be started with debug mode, defaults to false.')
    args = parser.parse_args()
    try:
        init_import_path()
        init_logger()
        init_flask("defaultConfig.json")
        g_log.info('init over.')

        start_listening()
    except Exception, err:
        print("[Error] : "), err
    else:
        print("Well done !")

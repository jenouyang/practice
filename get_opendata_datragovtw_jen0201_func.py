"""Download files from data.gov.tw
集中保管帳戶異動統計表
"""
from ast import literal_eval
import sys
import os
from io import StringIO
from datetime import datetime

import requests
import pandas as pd

source_json = ('https://quality.data.gov.tw/dq_download_json.php')
source_csv = ('https://quality.data.gov.tw/dq_download_csv.php')

url_params = {'nid': '11636', 'md5_url': '8d5715897dbe7767b520326686a0a950'}

proxies = {
    'http': 'http://proxy.esunsec.com.tw:8080',
    'https': 'http://proxy.esunsec.com.tw:8080'
}

outputpath_json = ('C://Users/S05964/Documents/MyPython38/export_data_json/json01.csv')
outputpath_csv = ('C://Users/S05964/Documents/MyPython38/export_data_csv/csv01.csv')


def check_status(status_code):
    """ check status code of the request
    """
    print(f'status code: {status_code}')
    if status_code != 200:
        raise requests.HTTPError
    return True


def check_isfile_json():
    # 檢查欲輸出的json檔案是否存在
    if os.path.isfile(outputpath_json):
        os.remove(outputpath_json)
        print("檔案存在,刪除既有json檔案")
    else:
        print("檔案不存在")
    return True


def check_isfile_csv():
    # 檢查欲輸出csv的檔案是否存在
    if os.path.isfile(outputpath_csv):
        os.remove(outputpath_csv)
        print("檔案存在,刪除既有csv檔案")
    else:
        print("檔案不存在")
    return True


def Get_json_to_csv():
    # 1-1.抓取DataGovTW json格式
    r = requests.get(source_json, params=url_params, proxies=proxies)
    print("1-1.抓取DataGovTW json格式")
    # 1-2.判斷requests狀態
    check_status(r.status_code)
    # 1-3.將取得的json內容轉為dictionary格式
    jsondata_dict = literal_eval(r.text)
    print("1-3.將取得的json內容轉為dictionary格式: ")
    print(jsondata_dict)
    # 1-4.將dictionary格式轉為df(表格)形式
    jsondata_df = pd.DataFrame(jsondata_dict)
    print("1-4.將dictionary格式轉為df(表格)形式: ")
    print(jsondata_df)
    # 1-5.檢查欲輸出的檔案是否存在
    check_isfile_json()
    # 1-6.將jsondata_df輸出csv檔案
    print("1-6.開始產檔")
    jsondata_df.to_csv(outputpath_json, sep=',', index=False, header=True)
    print("1-6.產檔完成")
    return True


def Get_csv_to_csv():
    # 2-1.抓取DataGovTWcsv格式
    r = requests.get(source_csv, params=url_params, proxies=proxies)
    print("2-1.抓取DataGovTW json格式")
    # 2-2.判斷requests狀態
    check_status(r.status_code)
    # 2-3.將取得的csv內容,使用StringIO讀出來
    #csvdata_StringIO = StringIO(r.text, newline='\n')
    csvdata_StringIO = StringIO(r.text)
    print("2-3.將取得的csv內容,使用StringIO讀出來:")
    print(csvdata_StringIO)
    # 2-4.將StringIO格式,使用df轉為表格形式
    csvdata_df = pd.read_csv(csvdata_StringIO)
    print("2-4.將StringIO格式,使用df轉為表格形式:")
    print(csvdata_df)
    # 2-5.檢查欲輸出的檔案是否存在
    check_isfile_csv()
    # 2-6.將csvdata_df輸出csv檔案
    print("2-6.開始產檔")
    csvdata_df.to_csv(outputpath_csv, sep=',', index=False, header=True)
    print("2-6.產檔完成")
    return


# 執行json作業
Get_json = Get_json_to_csv()

# 執行csv作業
Get_csv = Get_csv_to_csv()

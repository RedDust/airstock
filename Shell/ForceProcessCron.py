import os
import subprocess
import sys
import time
from datetime import datetime as DateTime, timedelta as TimeDelta
sys.path.append("D:/PythonProjects/airstock")
from multiprocessing import Process
from threading import Thread

from Realty.Test import thread_test_child

from Realty.Government import make_2_rent_statistics_data
from Realty.Auction import update_2_court_auction_geo_data_update_daily
from Realty.Naver import make_2_daily_statistics_data
from Realty.Auction import get_2_auction_sigu_info
from Realty.Auction import update_2_court_auction_update_build_type


dtNow = DateTime.today()
# print(dtNow.hour)
# print(dtNow.minute)
# print(dtNow)

intWeekDay = dtNow.weekday()

strBaseYYYY = str(dtNow.year).zfill(4)
strBaseMM = str(dtNow.month).zfill(2)
strBaseDD = str(dtNow.day).zfill(2)
strBaseHH = str(dtNow.hour).zfill(2)
strBaseII = str(dtNow.minute).zfill(2)
strBaseSS = str(dtNow.second).zfill(2)

strNowTime = strBaseHH + ":" + strBaseII + ":" + strBaseSS

print("[Process START : " + strNowTime + "] strBaseHH")
# [020101] 법원 경매 부동산 MASTER 테이블 위도경도 업데이트
from Realty.Auction import update_2_court_auction_geo_data_update_daily

print("[Process START : " + strNowTime + "] update_2_court_auction_geo_data_update_daily")
th1 = Thread(target=update_2_court_auction_geo_data_update_daily.main)
# th1.daemon = True
th1.start()
print("[Process END : " + strNowTime + "] update_2_court_auction_geo_data_update_daily")

# # [034160] 서울시 공동주택 아파트 정보
# from Realty.Government import get_1_seoul_apt_info_data_daily
#
# print("[Process START : " + strNowTime + "] get_1_seoul_apt_info_data_daily")
# th1 = Thread(target=get_1_seoul_apt_info_data_daily.main)
# # th1.daemon = True
# th1.start()
# print("[Process END : " + strNowTime + "] get_1_seoul_apt_info_data_daily")

# # [034190]지역 실거래 데이터 - 서울(41) 시설공사(90)
# from Realty.Government import get_1_seoul_sisul_data
#
# print("[Process START : " + strNowTime + "] get_1_seoul_sisul_data")
# th1 = Thread(target=get_1_seoul_sisul_data.main)
# # th1.daemon = True
# th1.start()
# print("[Process END : " + strNowTime + "] get_1_seoul_sisul_data")
#

# from Realty.Government import get_1_seoul_sisul_data
# th1 = Thread(target=get_1_seoul_sisul_data.main)
# # th1.daemon = True
# th1.start()

# # [020001] 법원경매 물건수집 - (결과)
# from Realty.Auction import get_1_court_auction_complete_curl
#
# print("[Process START : " + strNowTime + "] get_1_court_auction_complete_curl.py")
# th1 = Thread(target=get_1_court_auction_complete_curl.main)
# # th1.daemon = True
# th1.start()
# print("[Process END : " + strNowTime + "] get_1_court_auction_complete_curl.py")
# #
# #
# # [020000] 법원 경매 부동산 물건 수집(공통) -추후 지역 분할 가능
# from Realty.Auction import get_1_court_auction_curl
#
# print("[Process START : " + strNowTime + "] get_1_court_auction_curl")
# th1 = Thread(target=get_1_court_auction_curl.main)
# # th1.daemon = True
# th1.start()
# # print("[Process END : " + strNowTime + "] get_1_court_auction_curl")
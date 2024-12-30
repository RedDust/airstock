import os
import subprocess
import sys
import time
from datetime import datetime as DateTime, timedelta as TimeDelta
sys.path.append("D:/PythonProjects/airstock")
import logging
import logging.handlers
from multiprocessing import Process
from threading import Thread

from Init.Functions.Logs import GetLogDef

from Realty.Test import thread_test_child









dtNow = DateTime.today()
# print(dtNow.hour)
# print(dtNow.minute)
# print(dtNow)

logFileName = str(dtNow.year) + str(dtNow.month).zfill(2) + str(dtNow.day).zfill(2) + ".log"


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


formatter = logging.Formatter(u'%(asctime)s [%(levelname)8s] %(message)s')

streamingHandler = logging.StreamHandler()
streamingHandler.setFormatter(formatter)



# RotatingFileHandler
log_max_size = 10 * 1024 * 1024  ## 10MB
log_file_count = 20
rotatingFileHandler = logging.handlers.RotatingFileHandler(
    filename='D:/PythonProjects/airstock/Shell/logs/Cron2_'+logFileName,
    maxBytes=log_max_size,
    backupCount=log_file_count
)

rotatingFileHandler.setFormatter(formatter)
# RotatingFileHandler
timeFileHandler = logging.handlers.TimedRotatingFileHandler(
    filename='D:/PythonProjects/airstock/Shell/logs/Cron2_'+logFileName,
    when='midnight',
    interval=1,
    encoding='utf-8'
)
timeFileHandler.setFormatter(formatter)
logger.addHandler(streamingHandler)
logger.addHandler(timeFileHandler)

strBaseYYYY = str(dtNow.year).zfill(4)
strBaseMM = str(dtNow.month).zfill(2)
strBaseDD = str(dtNow.day).zfill(2)
strBaseHH = str(dtNow.hour).zfill(2)
strBaseII = str(dtNow.minute).zfill(2)
strBaseSS = str(dtNow.second).zfill(2)

strNowTime = strBaseHH + ":" + strBaseII + ":" + strBaseSS

logging.info("[CRONTAB START : "+strNowTime+"]=====================================")

# [034160] 서울시 공동주택 아파트 정보
from Realty.Government import get_1_seoul_apt_info_data_daily

print("[Process START : " + strNowTime + "] get_1_seoul_apt_info_data_daily")
th1 = Thread(target=get_1_seoul_apt_info_data_daily.main)
# th1.daemon = True
th1.start()
print("[Process END : " + strNowTime + "] get_1_seoul_apt_info_data_daily")
#
#



# #[042101] 법정동 API 수집
# from Realty.Government import get_1_korea_gov_code_info_api
# logging.info("[Process START : " + strNowTime + "] get_1_korea_gov_code_info_api")
# th1 = Thread(target=get_1_korea_gov_code_info_api.main)
# # th1.daemon = True
# th1.start()
# logging.info("[Process END : " + strNowTime + "] get_1_korea_gov_code_info_api")

# # #[041101] 어린이집별 기본정보 조회
# from Realty.Government import get_1_children_daycare_center_data
# logging.info("[Process START : " + strNowTime + "] get_1_children_daycare_center_data")
# th1 = Thread(target=get_1_children_daycare_center_data.main)
# # th1.daemon = True
# th1.start()
# logging.info("[Process END : " + strNowTime + "] get_1_children_daycare_center_data")


# # #[035301] 국토교통부 데이터 백업
# from Realty.Government import backup_3_korea_townhouse_trade_data_backup
# logging.info("[Process START : " + strNowTime + "] backup_3_korea_townhouse_trade_data_backup")
# th1 = Thread(target=backup_3_korea_townhouse_trade_data_backup.main)
# # th1.daemon = True
# th1.start()
# logging.info("[Process END : " + strNowTime + "] backup_3_korea_townhouse_trade_data_backup")


# #[040201] 카카오톡 메시지 발송 API
# from Realty.Service import naver_push_api
# logging.info("[Process START : " + strNowTime + "] naver_push_api")
# th1 = Thread(target=naver_push_api.main)
# # th1.daemon = True
# th1.start()
# logging.info("[Process END : " + strNowTime + "] naver_push_api")


# #[035151] 국토교통부_상업업무용 부동산 매매 신고 자료
# from Realty.Government import get_1_korea_business_trade_data_daily
# logging.info("[Process START : " + strNowTime + "] get_1_korea_business_trade_data_daily")
# th1 = Thread(target=get_1_korea_business_trade_data_daily.main)
# # th1.daemon = True
# th1.start()
# logging.info("[Process END : " + strNowTime + "] get_1_korea_business_trade_data_daily")
#
#
#
# #[035211] 국토교통부_아파트 임대차 신고 자료
# from Realty.Government import get_1_korea_apt_rent_data_daily
# logging.info("[Process START : " + strNowTime + "] get_1_korea_apt_rent_data_daily")
# th1 = Thread(target=get_1_korea_apt_rent_data_daily.main)
# # th1.daemon = True
# th1.start()
# logging.info("[Process END : " + strNowTime + "] get_1_korea_apt_rent_data_daily")

# #[035221] 국토교통부 단독/다가구 임대차 실거래가
# from Realty.Government import get_1_korea_house_rent_data_daily
# logging.info("[Process START : " + strNowTime + "] get_1_korea_house_rent_data_daily")
# th1 = Thread(target=get_1_korea_house_rent_data_daily.main)
# # th1.daemon = True
# th1.start()
# logging.info("[Process END : " + strNowTime + "] get_1_korea_house_rent_data_daily")
# #
#
# #[035111] 국토교통부_아파트 부동산 매매 신고 자료
# from Realty.Government import get_1_korea_trade_data_daily
# logging.info("[Process START : " + strNowTime + "] get_1_korea_trade_data_daily")
# th1 = Thread(target=get_1_korea_trade_data_daily.main)
# # th1.daemon = True
# th1.start()
# logging.info("[Process END : " + strNowTime + "] get_1_korea_trade_data_daily")


# # #[035141] 국토교통부_오피스텔 매매 실거래 자료
# from Realty.Government import get_1_korea_officetel_trade_data_daily
# logging.info("[Process START : " + strNowTime + "] get_1_korea_officetel_trade_data_daily")
# th1 = Thread(target=get_1_korea_officetel_trade_data_daily.main)
# # th1.daemon = True
# th1.start()
# logging.info("[Process END : " + strNowTime + "] get_1_korea_officetel_trade_data_daily")
#
#
# # #[035161] 국토교통부 토지 매매 실거래가
# from Realty.Government import get_1_korea_land_trade_data_daily
# logging.info("[Process START : " + strNowTime + "] get_1_korea_land_trade_data_daily")
# th1 = Thread(target=get_1_korea_land_trade_data_daily.main)
# # th1.daemon = True
# th1.start()
# logging.info("[Process END : " + strNowTime + "] get_1_korea_land_trade_data_daily")
#
#
# # #[035121] 국토교통부 단독/다가구 매매 실거래가
# from Realty.Government import get_1_korea_house_trade_data_daily
# logging.info("[Process START : " + strNowTime + "] get_1_korea_house_trade_data_daily")
# th1 = Thread(target=get_1_korea_house_trade_data_daily.main)
# # th1.daemon = True
# th1.start()
# logging.info("[Process END : " + strNowTime + "] get_1_korea_house_trade_data_daily")


# # [034160] 서울시 공동주택 아파트 정보
# from Realty.Government import get_1_seoul_apt_info_data_daily
# logging.info("[Process START : " + strNowTime + "] get_1_seoul_apt_info_data_daily")
# th1 = Thread(target=get_1_seoul_apt_info_data_daily.main)
# # th1.daemon = True
# th1.start()
# logging.info("[Process END : " + strNowTime + "] get_1_seoul_apt_info_data_daily")

# # #[035212] 국토교통부 아파트 임대차 통계 작성
# from Realty.Government import make_2_molit_apt_rent_daily_statistics
# logging.info("[Process START : " + strNowTime + "] make_2_molit_apt_rent_daily_statistics")
# th1 = Thread(target=make_2_molit_apt_rent_daily_statistics.main)
# # th1.daemon = True
# th1.start()
# logging.info("[Process END : " + strNowTime + "] make_2_molit_apt_rent_daily_statistics")
#
#
#
# # #[035112] 국토교통부 매매 거래 통계 작성
# from Realty.Government import make_2_molit_trade_daily_statistics
# logging.info("[Process START : " + strNowTime + "] make_2_molit_trade_daily_statistics")
# th1 = Thread(target=make_2_molit_trade_daily_statistics.main)
# # th1.daemon = True
# th1.start()
# logging.info("[Process END : " + strNowTime + "] make_2_molit_trade_daily_statistics")



# # #[035141] 국토교통부_오피스텔 매매 실거래 자료
# from Realty.Government import get_1_korea_officetel_trade_data_daily
# logging.info("[Process START : " + strNowTime + "] get_1_korea_officetel_trade_data_daily")
# th1 = Thread(target=get_1_korea_officetel_trade_data_daily.main)
# # th1.daemon = True
# th1.start()
# logging.info("[Process END : " + strNowTime + "] get_1_korea_officetel_trade_data_daily")






# # #[035241] 국토교통부 오피스텔 임대차 실거래가
# from Realty.Government import get_1_korea_officetel_rent_data_daily
# logging.info("[Process START : " + strNowTime + "] get_1_korea_officetel_rent_data_daily")
# th1 = Thread(target=get_1_korea_officetel_rent_data_daily.main)
# # th1.daemon = True
# th1.start()
# logging.info("[Process END : " + strNowTime + "] get_1_korea_officetel_rent_data_daily")
#
# #[035231] 국토교통부 연립주택 임대차 실거래가
# from Realty.Government import get_1_korea_townhouse_rent_data_daily
# logging.info("[Process START : " + strNowTime + "] get_1_korea_townhouse_rent_data_daily")
# th1 = Thread(target=get_1_korea_townhouse_rent_data_daily.main)
# # th1.daemon = True
# th1.start()
# logging.info("[Process END : " + strNowTime + "] get_1_korea_townhouse_rent_data_daily")
# #



#
#
#
# #[035131] 국토교통부_연립/다세대 매매 실거래 자료
# from Realty.Government import get_1_korea_townhouse_trade_data_daily
# logging.info("[Process START : " + strNowTime + "] get_1_korea_townhouse_trade_data_daily")
# th1 = Thread(target=get_1_korea_townhouse_trade_data_daily.main)
# # th1.daemon = True
# th1.start()
# logging.info("[Process END : " + strNowTime + "] get_1_korea_townhouse_trade_data_daily")
#
#


# # [040101] 날씨 정보 수집
# from Realty.Government import get_1_weather_daily
# logging.info("[Process START : " + strNowTime + "] get_1_weather_daily")
# th1 = Thread(target=get_1_weather_daily.main)
# # th1.daemon = True
# th1.start()
# logging.info("[Process END : " + strNowTime + "] get_1_weather_daily")

#[040201] 카카오톡 메시지 발송 API
# from Realty.Service import kakao_push_api
# logging.info("[Process START : " + strNowTime + "] kakao_push_api")
# th1 = Thread(target=kakao_push_api.main)
# # th1.daemon = True
# th1.start()
# logging.info("[Process END : " + strNowTime + "] kakao_push_api")
#
#


# # #

# #[020020] 법원경매 통계 데이터 작성
# from Realty.Auction import update_3_daily_stastistics
# logging.info("[Process START : " + strNowTime + "] update_3_daily_stastistics")
# th1 = Thread(target=update_3_daily_stastistics.main)
# # th1.daemon = True
# th1.start()
# logging.info("[Process END : " + strNowTime + "] update_3_daily_stastistics")

# #[020000] 법원 경매 부동산 물건 수집(공통) 수집
# from Realty.Auction import get_1_court_auction_curl
# logging.info("[Process START : " + strNowTime + "] get_1_court_auction_curl")
# th1 = Thread(target=get_1_court_auction_curl.main)
# # th1.daemon = True
# th1.start()
# logging.info("[Process END : " + strNowTime + "] get_1_court_auction_curl")


# #[020001] 법원경매 물건수집 - 매각 결과
# from Realty.Auction import get_1_court_auction_complete_curl
# logging.info("[Process START : " + strNowTime + "] get_1_court_auction_complete_curl.py")
# th1 = Thread(target=get_1_court_auction_complete_curl.main)
# # th1.daemon = True
# th1.start()
# logging.info("[Process END : " + strNowTime + "] get_1_court_auction_complete_curl.py")
# #
# #
# #[034110] 서울 실거래 매매 데이터 통계 작성
# from Realty.Government import make_2_trade_statistics_data
# logging.info("[Process START : " + strNowTime + "] make_2_trade_statistics_data")
# th1 = Thread(target=make_2_trade_statistics_data.main)
# # th1.daemon = True
# th1.start()
# logging.info("[Process END : " + strNowTime + "] make_2_trade_statistics_data")

#
#
# #[034210] 서울 실거래 렌트 데이터 통계 작성
from Realty.Government import make_2_rent_statistics_data
# logging.info("[Process START : " + strNowTime + "] make_2_rent_statistics_data")
# th1 = Thread(target=make_2_rent_statistics_data.main)
# # th1.daemon = True
# th1.start()
# logging.info("[Process END : " + strNowTime + "] make_2_rent_statistics_data")


#
# # #[035113] 국토교통부 매매 거래 통계 작성
# from Realty.Government import make_2_molit_apt_trade_statistics_year
# logging.info("[Process START : " + strNowTime + "] make_2_molit_apt_trade_statistics_year")
# th1 = Thread(target=make_2_molit_apt_trade_statistics_year.main)
# # th1.daemon = True
# th1.start()
# logging.info("[Process END : " + strNowTime + "] make_2_molit_apt_trade_statistics_year")


# # 법원경매 건물타입 업데이트
from Realty.Auction import update_2_court_auction_update_build_type
# logging.info("[Process START : " + strNowTime + "] update_2_court_auction_update_build_type.py")
# th1 = Thread(target=update_2_court_auction_update_build_type.main)
# # th1.daemon = True
# th1.start()
# logging.info("[Process END : " + strNowTime + "] update_2_court_auction_update_build_type.py")
#


# # [022002] 법원경매 법정 시군구 정보 수집
from Realty.Auction import get_2_auction_sigu_info
# from Realty.Auction import get_2_auction_sigu_info
# logging.info("[Process START : " + strNowTime + "] get_2_auction_sigu_info")
# th1 = Thread(target=get_2_auction_sigu_info.main)
# # th1.daemon = True
# th1.start()
# logging.info("[Process END : " + strNowTime + "] get_2_auction_sigu_info")

#
# #[022001] 법원경매 법정 시도 정보 수집
# from Realty.Auction import get_1_auction_sido_info
# logging.info("[Process START : " + strNowTime + "] get_1_auction_sido_info")
# th1 = Thread(target=get_1_auction_sido_info.main)
# # th1.daemon = True
# th1.start()
# logging.info("[Process END : " + strNowTime + "] get_1_auction_sido_info")



# #[020103] 법원 경매 물건별 주소 변환 테이블 작성
# from Realty.Government import get_1_address_for_auction
# logging.info("[Process START : " + strNowTime + "] get_1_address_for_auction")
# th1 = Thread(target=get_1_address_for_auction.main)
# # th1.daemon = True
# th1.start()
# logging.info("[Process END : " + strNowTime + "] get_1_address_for_auction")

#
# from Realty.Government import get_1_seoul_apt_info_data_daily
# logging.info("[Process START : " + strNowTime + "] get_1_seoul_apt_info_data_daily")
# th1 = Thread(target=get_1_seoul_apt_info_data_daily.main)
# # th1.daemon = True
# th1.start()
# logging.info("[Process END : " + strNowTime + "] get_1_seoul_apt_info_data_daily")



# #[011010] 네이버 물건 월별 백업 및 마스터 삭제
# from Realty.Naver import backup_2_rand_data_for_month
# logging.info("[Process START : " + strNowTime + "] backup_2_rand_data_for_month")
# th1 = Thread(target=backup_2_rand_data_for_month.main)
# # th1.daemon = True
# th1.start()
# logging.info("[Process END : " + strNowTime + "] backup_2_rand_data_for_month")





# #[090000] 테스트 수집
from Realty.Auction import get_1_court_auction_by_location_curl
# logging.info("[Process START : " + strNowTime + "] get_1_court_auction_by_location_curl")
# th1 = Thread(target=get_1_court_auction_by_location_curl.main)
# # th1.daemon = True
# th1.start()
# logging.info("[Process END : " + strNowTime + "] get_1_court_auction_by_location_curl")
#
#


# # [020101] 법원 경매 부동산 MASTER 테이블 위도경도 업데이트
from Realty.Auction import update_2_court_auction_geo_data_update_daily
# logging.info("[Process START : " + strNowTime + "] update_2_court_auction_geo_data_update_daily")
# th1 = Thread(target=update_2_court_auction_geo_data_update_daily.main)
# # th1.daemon = True
# th1.start()
# logging.info("[Process END : " + strNowTime + "] update_2_court_auction_geo_data_update_daily")
# # #



# #[100000] 테스트 프로그램
# from Realty.Test import get_temp_test
# logging.info("[Process START : " + strNowTime + "] get_temp_test")
# th1 = Thread(target=get_temp_test.main)
# # th1.daemon = True
# th1.start()
# logging.info("[Process END : " + strNowTime + "] get_temp_test")
#





# # #[012000] 네이버 부동산 일별 통계 데이터 작성
from Realty.Naver import make_2_daily_statistics_data
# logging.info("[Process START : " + strNowTime + "] make_2_daily_statistics_data")
# th1 = Thread(target=make_2_daily_statistics_data.main)
# # th1.daemon = True
# th1.start()
# logging.info("[Process END : " + strNowTime + "] make_2_daily_statistics_data")



# from Realty.Government import get_1_seoul_apt_info_data_daily
# logging.info("[Process START : " + strNowTime + "] get_1_seoul_apt_info_data_daily")
# th1 = Thread(target=get_1_seoul_apt_info_data_daily.main)
# # th1.daemon = True
# th1.start()
# logging.info("[Process END : " + strNowTime + "] get_1_seoul_apt_info_data_daily")


# #[034190] 서울 부동산 실거래가 데이터 - 임대차
# from Realty.Government import get_1_seoul_sisul_data
# logging.info("[Process START : " + strNowTime + "] get_1_seoul_sisul_data")
# th1 = Thread(target=get_1_seoul_sisul_data.main)
# # th1.daemon = True
# th1.start()
# logging.info("[Process END : " + strNowTime + "] get_1_seoul_sisul_data")
#


# logging.info("[CRONTAB END : "+strNowTime+"]=====================================")

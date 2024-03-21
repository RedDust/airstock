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
from Realty.Government import make_2_trade_statistics_data
from Realty.Government import make_2_rent_statistics_data
from Realty.Auction import update_2_court_auction_geo_data_update_daily
from Realty.Auction import get_1_court_auction_curl
from Realty.Naver import make_2_daily_statistics_data
from Realty.Auction import get_1_court_auction_complete_curl
from Realty.Government import get_1_weather_daily


dtNow = DateTime.today()
# print(dtNow.hour)
# print(dtNow.minute)
# print(dtNow)

logFileName = str(dtNow.year) + str(dtNow.month).zfill(2)  + str(dtNow.day).zfill(2)  + ".log"


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


formatter = logging.Formatter(u'%(asctime)s [%(levelname)8s] %(message)s')

streamingHandler = logging.StreamHandler()
streamingHandler.setFormatter(formatter)



# RotatingFileHandler
log_max_size = 10 * 1024 * 1024  ## 10MB
log_file_count = 20
rotatingFileHandler = logging.handlers.RotatingFileHandler(
    filename='D:/PythonProjects/airstock/Shell/logs/Cron_'+logFileName,
    maxBytes=log_max_size,
    backupCount=log_file_count
)
rotatingFileHandler.setFormatter(formatter)
# RotatingFileHandler
timeFileHandler = logging.handlers.TimedRotatingFileHandler(
    filename='D:/PythonProjects/airstock/Shell/logs/Cron_'+logFileName,
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


if strBaseHH == "00":
    if strBaseII == "00":
        # [040101] 날씨 정보 수집
        logging.info("[Process START : " + strNowTime + "] get_1_weather_daily")
        th1 = Thread(target=get_1_weather_daily.main)
        # th1.daemon = True
        th1.start()
        logging.info("[Process END : " + strNowTime + "] get_1_weather_daily")


elif strBaseHH == "01":
    if strBaseII == "10":
        # 01시 10분
        logging.info("[Process START : " + strNowTime + "] make_2_trade_statistics_data")
        th1 = Thread(target=make_2_trade_statistics_data.main)
        # th1.daemon = True
        th1.start()
        logging.info("[Process END : " + strNowTime + "] make_2_trade_statistics_data")

    if strBaseII == "20":
        # 01시 20분
        logging.info("[Process START : " + strNowTime + "] make_2_rent_statistics_data")
        th1 = Thread(target=make_2_rent_statistics_data.main)
        # th1.daemon = True
        th1.start()
        logging.info("[Process END : " + strNowTime + "] make_2_rent_statistics_data")

if strBaseHH == "02":
    if strBaseII == "00":
        # [040101] 날씨 정보 수집
        logging.info("[Process START : " + strNowTime + "] get_1_weather_daily")
        th1 = Thread(target=get_1_weather_daily.main)
        # th1.daemon = True
        th1.start()
        logging.info("[Process END : " + strNowTime + "] get_1_weather_daily")

elif strBaseHH == "05":
    if strBaseII == "00":
        # [040101] 날씨 정보 수집
        logging.info("[Process START : " + strNowTime + "] get_1_weather_daily")
        th1 = Thread(target=get_1_weather_daily.main)
        # th1.daemon = True
        th1.start()
        logging.info("[Process END : " + strNowTime + "] get_1_weather_daily")


elif strBaseHH == "06":
    if strBaseII == "10":
        # 07시 00분
        logging.info("[Process START : " + strNowTime + "] get_1_court_auction_curl")
        th1 = Thread(target=get_1_court_auction_curl.main)
        # th1.daemon = True
        th1.start()
        logging.info("[Process END : " + strNowTime + "] get_1_court_auction_curl")

    if strBaseII == "20":
        #[020102] 법원경매 건물타입 업데이트
        logging.info("[Process START : " + strNowTime + "] get_1_court_auction_complete_curl.py")
        th1 = Thread(target=get_1_court_auction_complete_curl.main)
        # th1.daemon = True
        th1.start()
        logging.info("[Process END : " + strNowTime + "] get_1_court_auction_complete_curl.py")
        #


elif strBaseHH == "07":

    if strBaseII == "10":
        #[020020] 법원경매 통계 데이터 작성
        from Realty.Auction import update_3_daily_stastistics
        logging.info("[Process START : " + strNowTime + "] update_3_daily_stastistics")
        th1 = Thread(target=update_3_daily_stastistics.main)
        # th1.daemon = True
        th1.start()
        logging.info("[Process END : " + strNowTime + "] update_3_daily_stastistics")

    if strBaseII == "20":
        # 07시 20분
        logging.info("[Process START : " + strNowTime + "] make_2_daily_statistics_data")
        th1 = Thread(target=make_2_daily_statistics_data.main)
        # th1.daemon = True
        th1.start()
        logging.info("[Process END : " + strNowTime + "] make_2_daily_statistics_data")


    if strBaseII == "20":
        # 08시 00분
        logging.info("[Process START : " + strNowTime + "] update_2_court_auction_geo_data_update_daily")
        th1 = Thread(target=update_2_court_auction_geo_data_update_daily.main)
        # th1.daemon = True
        th1.start()
        logging.info("[Process END : " + strNowTime + "] update_2_court_auction_geo_data_update_daily")


elif strBaseHH == "12":
    if strBaseII == "00":
        # [040101] 날씨 정보 수집
        logging.info("[Process START : " + strNowTime + "] get_1_weather_daily")
        th1 = Thread(target=get_1_weather_daily.main)
        # th1.daemon = True
        th1.start()
        logging.info("[Process END : " + strNowTime + "] get_1_weather_daily")



elif strBaseHH == "14":
    if strBaseII == "00":
        # [040101] 날씨 정보 수집
        logging.info("[Process START : " + strNowTime + "] get_1_weather_daily")
        th1 = Thread(target=get_1_weather_daily.main)
        # th1.daemon = True
        th1.start()
        logging.info("[Process END : " + strNowTime + "] get_1_weather_daily")

elif strBaseHH == "16":
    if strBaseII == "00":
        # [040101] 날씨 정보 수집
        logging.info("[Process START : " + strNowTime + "] get_1_weather_daily")
        th1 = Thread(target=get_1_weather_daily.main)
        # th1.daemon = True
        th1.start()
        logging.info("[Process END : " + strNowTime + "] get_1_weather_daily")

elif strBaseHH == "18":
    if strBaseII == "00":
        # [040101] 날씨 정보 수집
        logging.info("[Process START : " + strNowTime + "] get_1_weather_daily")
        th1 = Thread(target=get_1_weather_daily.main)
        # th1.daemon = True
        th1.start()
        logging.info("[Process END : " + strNowTime + "] get_1_weather_daily")

elif strBaseHH == "20":
    if strBaseII == "00":
        # [040101] 날씨 정보 수집
        logging.info("[Process START : " + strNowTime + "] get_1_weather_daily")
        th1 = Thread(target=get_1_weather_daily.main)
        # th1.daemon = True
        th1.start()
        logging.info("[Process END : " + strNowTime + "] get_1_weather_daily")

elif strBaseHH == "22":
    if strBaseII == "00":
        # [040101] 날씨 정보 수집
        logging.info("[Process START : " + strNowTime + "] get_1_weather_daily")
        th1 = Thread(target=get_1_weather_daily.main)
        # th1.daemon = True
        th1.start()
        logging.info("[Process END : " + strNowTime + "] get_1_weather_daily")




logging.info("[CRONTAB END : "+strNowTime+"]=====================================")

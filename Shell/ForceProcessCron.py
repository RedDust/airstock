import os
import subprocess
import sys
import time
from datetime import datetime as DateTime, timedelta as TimeDelta
sys.path.append("D:/PythonProjects/airstock")
from multiprocessing import Process
from threading import Thread

dtNow = DateTime.today()

intWeekDay = dtNow.weekday()

strBaseYYYY = str(dtNow.year).zfill(4)
strBaseMM = str(dtNow.month).zfill(2)
strBaseDD = str(dtNow.day).zfill(2)
strBaseHH = str(dtNow.hour).zfill(2)
strBaseII = str(dtNow.minute).zfill(2)
strBaseSS = str(dtNow.second).zfill(2)

strNowTime = strBaseHH + ":" + strBaseII + ":" + strBaseSS

print("[Process START : " + strNowTime + "] strBaseHH")

# # [034180] 지역 데이터 - 서울(41) 지하철 사용량(80)
from Realty.Government import get_1_seoul_subway_station_geo_data

print("[Process START : " + strNowTime + "] get_1_seoul_subway_station_geo_data")
th1 = Thread(target=get_1_seoul_subway_station_geo_data.main)
# th1.daemon = True
th1.start()
print("[Process END : " + strNowTime + "] get_1_seoul_subway_station_geo_data")

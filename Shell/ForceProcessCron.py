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


dtNow = DateTime.today()
# print(dtNow.hour)
# print(dtNow.minute)
# print(dtNow)

logFileName = str(dtNow.year) + str(dtNow.month) + str(dtNow.day) + ".log"


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


# logging.info("[Process START : " + strNowTime + "] make_2_trade_statistics_data")
# th1 = Thread(target=make_2_trade_statistics_data.main)
# # th1.daemon = True
# th1.start()
# logging.info("[Process END : " + strNowTime + "] make_2_trade_statistics_data")


# logging.info("[Process START : " + strNowTime + "] make_2_rent_statistics_data")
# th1 = Thread(target=make_2_rent_statistics_data.main)
# # th1.daemon = True
# th1.start()
# logging.info("[Process END : " + strNowTime + "] make_2_rent_statistics_data")
#


logging.info("[Process START : " + strNowTime + "] update_2_court_auction_geo_data_update_daily")
th1 = Thread(target=update_2_court_auction_geo_data_update_daily.main)
# th1.daemon = True
th1.start()
logging.info("[Process END : " + strNowTime + "] update_2_court_auction_geo_data_update_daily")







logging.info("[CRONTAB END : "+strNowTime+"]=====================================")

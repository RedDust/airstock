#
import requests


# This is a sample Python script.
import sys
import json
import time
import random
import pymysql
import logging
import logging.handlers
import inspect
import traceback
from datetime import datetime as DateTime, timedelta as TimeDelta

def logInit(strProcessType):


    dtNow = DateTime.today()
    # print(dtNow.hour)
    # print(dtNow.minute)
    # print(dtNow)

    logFileName = str(dtNow.year) + str(dtNow.month) + str(dtNow.day).zfill(2) + ".log"

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(u'%(asctime)s [%(levelname)8s] %(message)s')

    streamingHandler = logging.StreamHandler()
    streamingHandler.setFormatter(formatter)

    # RotatingFileHandler
    log_max_size = 10 * 1024 * 1024  ## 10MB
    log_file_count = 20

    # RotatingFileHandler
    timeFileHandler = logging.handlers.TimedRotatingFileHandler(
        filename='D:/PythonProjects/airstock/Shell/logs/' + strProcessType + '_get_auction_' + logFileName,
        when='midnight',
        interval=1,
        encoding='utf-8'
    )
    timeFileHandler.setFormatter(formatter)
    logger.addHandler(streamingHandler)
    logger.addHandler(timeFileHandler)

    return True

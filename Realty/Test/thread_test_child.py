import os
import sys
import time

sys.path.append("D:/PythonProjects/airstock")
import urllib.request
import json
import pymysql
import traceback
from datetime import datetime as DateTime, timedelta as TimeDelta

from Init.Functions.Logs import GetLogDef

def test_thread(nStartCount ,nEndCount):
    proc = os.getpid()
    # nBaseDate = 20221231
    dtToday = DateTime.today()

    print(GetLogDef.lineno(), proc,  "dtToday >", type(dtToday), dtToday)
    print(GetLogDef.lineno(), proc,  "nStartCount >", type(nStartCount), nStartCount)
    time.sleep(3600)
    print(GetLogDef.lineno(), proc,  "nEndCount >", type(nEndCount), nEndCount)


def test_noargs():
    proc = os.getpid()
    # nBaseDate = 20221231
    dtToday = DateTime.today()
    print(GetLogDef.lineno(), proc,  "--------------------------------------------------------")
    print(GetLogDef.lineno(), proc,  "proc >", type(proc), proc)
    print(GetLogDef.lineno(), proc,  "dtToday >", type(dtToday), dtToday)
    time.sleep(60)
    dtToday2 = DateTime.today()
    print(GetLogDef.lineno(), proc,  "nEndCount >", type(dtToday2), dtToday2)




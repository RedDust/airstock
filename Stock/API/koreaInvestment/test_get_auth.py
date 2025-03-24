

from Stock.API.koreaInvestment.Lib import kis_auth as ka, kis_domstk as kb

import inspect
import logging
import logging.handlers
from Init.Functions.Logs import GetLogDef

import pandas as pd
from datetime import datetime
from datetime import timedelta
import sys

# 토큰 발급
ka.auth()
strStockCode = "003920"



# # [국내주식] 주문/계좌 > 주식잔고조회 (보유종목리스트)
# rt_data = kb.get_inquire_balance_lst()
# print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
#                                        inspect.getframeinfo(inspect.currentframe()).lineno), "rt_data >> " , type(rt_data), rt_data)    # 현재가, 전일대비
# print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
#                                        inspect.getframeinfo(inspect.currentframe()).lineno), "==================================================================>>")    # 현재가, 전일대비
#
#
# dictRtDatas = pd.DataFrame(rt_data)
# dictValues = dictRtDatas.to_dict()
# print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
#                         inspect.getframeinfo(inspect.currentframe()).lineno), "dictValue >> ",  len(dictValues), type(dictValues),
#        dictValues)  # 현재가, 전일대비
# #
# for key in dictValues:
#
#     print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
#                             inspect.getframeinfo(inspect.currentframe()).lineno), "key >> ", type(key),
#           key)  # 현재가, 전일대비
#
#     listValue = dictValues[key]
#
#     print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
#                             inspect.getframeinfo(inspect.currentframe()).lineno), "listValue >> ",  len(listValue), type(listValue),
#           key , listValue)  # 현재가, 전일대비
#

# [국내주식] 기본시세 > 주식현재가 시세 (종목번호 6자리)
rt_data = kb.get_inquire_price(itm_no=strStockCode)
print(rt_data)    # 현재가, 전일대비
dictRtDatas = pd.DataFrame(rt_data)
dictValues = dictRtDatas.to_dict()

dictKoreaInvestDatas = dict()

for dictkey , dictValue in dictValues.items():
    # print(dictkey, "=> " ,dictValue)
    dictKoreaInvestDatas[dictkey] = dictValue[0]

print(dictKoreaInvestDatas)

# print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
#                                        inspect.getframeinfo(inspect.currentframe()).lineno), "==================================================================>>")    # 현재가, 전일대비
# # [국내주식] 기본시세 > 주식현재가 투자자 (종목번호 6자리)
# rt_data = kb.get_inquire_time_itemconclusion(output_dv='3',itm_no=strStockCode,inqr_hour='154500')
#
# dictRtDatas = pd.DataFrame(rt_data)
# dictValues = dictRtDatas.to_dict()
# #
# for key in dictValues:
#
#     listValue = dictValues[key]
#     print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
#                             inspect.getframeinfo(inspect.currentframe()).lineno), "listValue >> ",  len(listValue), type(listValue),
#           key , listValue)  # 현재가, 전일대비
#
#

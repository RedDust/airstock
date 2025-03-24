#__init__.py
import traceback
import pandas as pd
import json

def dict_koreainvest_get_qoutes(strSectorCode):

    import inspect as Isp, logging, logging.handlers
    from Init.Functions.Logs import GetLogDef as SLog
    from Stock.LIB.Logging import UnifiedLogDeclarationFunction as ULF
    import Stock.API.koreaInvestment.Lib.kis_auth as ka
    import Stock.API.koreaInvestment.Lib.kis_domstk as kb

    try:

        if len(strSectorCode) != 6:
            raise Exception(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +strSectorCode )

        print("Call Now get_now_qoutes")
        # [국내주식] 기본시세 > 주식현재가 호가 (종목번호 6자리)
        rt_data = kb.get_inquire_asking_price_exp_ccn(itm_no=strSectorCode)

        dictRtDatas = pd.DataFrame(rt_data)
        dictValues = dictRtDatas.to_dict()

        # print("dictValues => ", dictValues)

        # SELL (매도)

        dictReturn = dict()



        dictReturn['sell']= dict()
        dictReturn['sell'][0] = dict()
        dictReturn['sell'][0]['qoute'] = int(dictValues['askp1'][0])
        dictReturn['sell'][0]['volumn'] = int(dictValues['askp_rsqn1'][0])
        dictReturn['sell'][0]['amount'] = int(dictValues['askp1'][0]) * int(dictValues['askp_rsqn1'][0])

        dictReturn['sell'][1] = dict()
        dictReturn['sell'][1]['qoute'] = int(dictValues['askp2'][0])
        dictReturn['sell'][1]['volumn'] = int(dictValues['askp_rsqn2'][0])
        dictReturn['sell'][1]['amount'] = int(dictValues['askp2'][0]) * int(dictValues['askp_rsqn2'][0])

        dictReturn['sell'][2] = dict()
        dictReturn['sell'][2]['qoute'] = int(dictValues['askp3'][0])
        dictReturn['sell'][2]['volumn'] = int(dictValues['askp_rsqn3'][0])
        dictReturn['sell'][2]['amount'] = int(dictValues['askp3'][0]) * int(dictValues['askp_rsqn3'][0])

        dictReturn['sell'][3] = dict()
        dictReturn['sell'][3]['qoute'] = int(dictValues['askp4'][0])
        dictReturn['sell'][3]['volumn'] = int(dictValues['askp_rsqn4'][0])
        dictReturn['sell'][3]['amount'] = int(dictValues['askp4'][0]) * int(dictValues['askp_rsqn4'][0])

        dictReturn['sell'][4] = dict()
        dictReturn['sell'][4]['qoute'] = int(dictValues['askp5'][0])
        dictReturn['sell'][4]['volumn'] = int(dictValues['askp_rsqn5'][0])
        dictReturn['sell'][4]['amount'] = int(dictValues['askp5'][0]) * int(dictValues['askp_rsqn5'][0])

        dictReturn['buy']= dict()
        dictReturn['buy'][0] = dict()
        dictReturn['buy'][0]['qoute'] = int(dictValues['bidp1'][0])
        dictReturn['buy'][0]['volumn'] = int(dictValues['bidp_rsqn1'][0])
        dictReturn['buy'][0]['amount'] = int(dictValues['bidp1'][0]) * int(dictValues['bidp_rsqn1'][0])

        dictReturn['buy'][1] = dict()
        dictReturn['buy'][1]['qoute'] = int(dictValues['bidp2'][0])
        dictReturn['buy'][1]['volumn'] = int(dictValues['bidp_rsqn2'][0])
        dictReturn['buy'][1]['amount'] = int(dictValues['bidp2'][0]) * int(dictValues['bidp_rsqn2'][0])

        dictReturn['buy'][2] = dict()
        dictReturn['buy'][2]['qoute'] = int(dictValues['bidp3'][0])
        dictReturn['buy'][2]['volumn'] = int(dictValues['bidp_rsqn3'][0])
        dictReturn['buy'][2]['amount'] = int(dictValues['bidp3'][0]) * int(dictValues['bidp_rsqn3'][0])

        dictReturn['buy'][3] = dict()
        dictReturn['buy'][3]['qoute'] = int(dictValues['bidp4'][0])
        dictReturn['buy'][3]['volumn'] = int(dictValues['bidp_rsqn4'][0])
        dictReturn['buy'][3]['amount'] = int(dictValues['bidp4'][0]) * int(dictValues['bidp_rsqn4'][0])

        dictReturn['buy'][4] = dict()
        dictReturn['buy'][4]['qoute'] = int(dictValues['bidp5'][0])
        dictReturn['buy'][4]['volumn'] = int(dictValues['bidp_rsqn5'][0])
        dictReturn['buy'][4]['amount'] = int(dictValues['bidp5'][0]) * int(dictValues['bidp_rsqn5'][0])


        dictReturn['sum'] = dict()




        dictReturn['sum']['sell'] = dict()
        dictReturn['sum']['buy'] = dict()


        # print("dictReturn" , dictReturn)

        return dictReturn



        # intaskp_rsqn1 = int(dictValues['askp_rsqn1'][0])
        # intaskp_rsqn2 = int(dictValues['askp_rsqn2'][0])
        # intaskp_rsqn3 = int(dictValues['askp_rsqn3'][0])
        # intaskp_rsqn4 = int(dictValues['askp_rsqn4'][0])
        # intaskp_rsqn5 = int(dictValues['askp_rsqn5'][0])
        #
        # intaskp1 = int(dictValues['askp1'][0])
        # intaskp2 = int(dictValues['askp2'][0])
        # intaskp3 = int(dictValues['askp3'][0])
        # intaskp4 = int(dictValues['askp4'][0])
        # intaskp5 = int(dictValues['askp5'][0])
        #
        # # BUY (매수)
        # intbidp_rsqn1 = int(dictValues['bidp_rsqn1'][0])
        # intbidp_rsqn2 = int(dictValues['bidp_rsqn2'][0])
        # intbidp_rsqn3 = int(dictValues['bidp_rsqn3'][0])
        # intbidp_rsqn4 = int(dictValues['bidp_rsqn4'][0])
        # intbidp_rsqn5 = int(dictValues['bidp_rsqn5'][0])
        #
        # intbidp1 = int(dictValues['bidp1'][0])
        # intbidp2 = int(dictValues['bidp2'][0])
        # intbidp3 = int(dictValues['bidp3'][0])
        # intbidp4 = int(dictValues['bidp4'][0])
        # intbidp5 = int(dictValues['bidp5'][0])
        #
        # intTotalSellVolumn = intaskp_rsqn1 + intaskp_rsqn2 + intaskp_rsqn3 + intaskp_rsqn4
        #
        # intTotalSellAmount1 = (intaskp_rsqn1 * intaskp1)
        # intTotalSellAmount2 = (intaskp_rsqn2 * intaskp2)
        # intTotalSellAmount3 = (intaskp_rsqn3 * intaskp3)
        # intTotalSellAmount4 = (intaskp_rsqn4 * intaskp4)
        # intTotalSellAmount5 = (intaskp_rsqn5 * intaskp5)
        #
        # intTotalBuyAmount1 = (intbidp_rsqn1 * intbidp1)
        # intTotalBuyAmount2 = (intbidp_rsqn2 * intbidp2)
        # intTotalBuyAmount3 = (intbidp_rsqn3 * intbidp3)
        # intTotalBuyAmount4 = (intbidp_rsqn4 * intbidp4)
        # intTotalBuyAmount5 = (intbidp_rsqn5 * intbidp5)
        #
        # print("intaskp1=>", intaskp1)
        # print("intaskp_rsqn1=>", intaskp_rsqn1)
        # print("intTotalSellAmount1=>", intTotalSellAmount1)
        # print("intbidp1=>", intbidp1)
        # print("intbidp_rsqn1=>", intbidp_rsqn1)
        # print("intTotalBuyAmount1=>", intTotalBuyAmount1)

    except Exception as e:
        print(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " Error Exception")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + str(e))
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + str(type(e)))
        err_msg = str(traceback.format_exc())
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + str(err_msg))
    else:
        logging.info(SLog.Ins(Isp.getframeinfo,
                          Isp.currentframe()) + "[ELSE]========================================================")

    finally:
        logging.info(SLog.Ins(Isp.getframeinfo,
                          Isp.currentframe()) + "[Finally END]========================================================")

from Init.Functions.Logs import GetLogDef
from Lib.CryptoModule import AesCrypto
from Lib.CustomException import QuitException
import traceback

def MakeUniqueValue(SelectColumnList):

    try:
        print("MakeUniqueValue START")

        nAuctionCode = str(SelectColumnList.get('auction_code'))
        nAuctionSeq = str(SelectColumnList.get('auction_seq'))
        strCourtName = str(SelectColumnList.get('court_name'))
        dtAuctionDay = str(SelectColumnList.get('auction_day'))

        strUniqueValue = nAuctionCode + "_" + nAuctionSeq + "_" + strCourtName + dtAuctionDay

        aes = AesCrypto.AESCipher('aesKey')
        strUniqueValueEnc = aes.encrypt(strUniqueValue)



    except QuitException as e:
        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)


        print(GetLogDef.lineno(__file__), "QuitException")
        err_msg = traceback.format_exc()
        print(err_msg)
        print(e)
        print(type(e))
        return False
    except Exception as e:
        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)

        print(GetLogDef.lineno(__file__), "Error Exception")
        print(GetLogDef.lineno(__file__), e, type(e))
        err_msg = traceback.format_exc()
        print(err_msg)
        return False
    else:
        print(GetLogDef.lineno(__file__), "MAKE SUCCESS ============================================================")
        return strUniqueValueEnc
    finally:
        print("MakeUniqueValue END")


def MakeUniqueValue2(SelectColumnList):

    try:
        print("MakeUniqueValue2 START")

        nAuctionCode = str(SelectColumnList.get('auction_code'))
        nAuctionSeq = str(SelectColumnList.get('auction_seq'))
        strCourtName = str(SelectColumnList.get('court_name'))
        dtAuctionDay = str(SelectColumnList.get('auction_day'))
        strAuctionType = str(SelectColumnList.get('auction_type'))

        strUniqueValue2 = nAuctionCode + "_" + nAuctionSeq + "_" + strCourtName + dtAuctionDay + "_" + strAuctionType

        aes = AesCrypto.AESCipher('aesKey')
        strUniqueValue2Enc = aes.encrypt(strUniqueValue2)



    except QuitException as e:
        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)


        print(GetLogDef.lineno(__file__), "QuitException")
        err_msg = traceback.format_exc()
        print(err_msg)
        print(e)
        print(type(e))
        return False
    except Exception as e:
        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)

        print(GetLogDef.lineno(__file__), "Error Exception")
        print(GetLogDef.lineno(__file__), e, type(e))
        err_msg = traceback.format_exc()
        print(err_msg)
        return False
    else:
        print(GetLogDef.lineno(__file__), "MAKE SUCCESS ============================================================")
        return strUniqueValue2Enc
    finally:
        print("MakeUniqueValue2 END")
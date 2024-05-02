
from Init.Functions.Logs import GetLogDef
from Realty.Auction.Const.AuctionCourtInfo import arrCourtName
import inspect, re

def DecodeIssueNumber(strIssueNumberText):

    print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                            inspect.getframeinfo(inspect.currentframe()).lineno), len(strIssueNumberText),
          strIssueNumberText)

    strStripFieldName = re.sub(r"[^\uAC00-\uD7A30-9a-zA-Z-,\s]", "", strIssueNumberText)
    print("listStripFieldName" , len(strStripFieldName), strStripFieldName)


    listIssueNumberTexts = str(strStripFieldName).split(",")

    for listIssueNumberText in listIssueNumberTexts:
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), len(listIssueNumberText), listIssueNumberText)

    strResult = str(listIssueNumberTexts[0]).strip()+'|'+str(listIssueNumberTexts[1].strip())

    return strResult



def trim_msg(str_msg, int_max_len=80, encoding='euc-kr'):
    try:
        return str_msg.encode(encoding)[:int_max_len].decode(encoding)
    except UnicodeDecodeError:
        try:
            return str_msg.encode(encoding)[:int_max_len-1].decode(encoding)
        except UnicodeDecodeError:
            return str_msg.encode(encoding)[:int_max_len-2].decode(encoding)

#initialization of constants
# 사용안하기로한 DICT
from typing import List, Any

dictNaverTypes = dict()
dictNaverTypes['A01'] = {"1": 0, "2": 0, "3":0,"4":0}
dictNaverTypes['A02'] = {"1": 0, "2": 0, "3": 0, "4": 0}
dictNaverTypes['A04'] = {"1": 0, "2": 0, "3": 0, "4": 0}
dictNaverTypes['C02'] = {"1": 0, "2": 0, "3": 0, "4": 0}
dictNaverTypes['C03'] = {"1": 0, "2": 0, "3": 0, "4": 0}
dictNaverTypes['C04'] = {"1": 0, "2": 0, "3": 0, "4": 0}
dictNaverTypes['C06'] = {"1": 0, "2": 0, "3": 0, "4": 0}
dictNaverTypes['D01'] = {"1": 0, "2": 0, "3": 0, "4": 0}
dictNaverTypes['D02'] = {"1": 0, "2": 0, "3": 0, "4": 0}
dictNaverTypes['D03'] = {"1": 0, "2": 0, "3": 0, "4": 0}
dictNaverTypes['D04'] = {"1": 0, "2": 0, "3": 0, "4": 0}
dictNaverTypes['D05'] = {"1": 0, "2": 0, "3": 0, "4": 0}
dictNaverTypes['E01'] = {"1": 0, "2": 0, "3": 0, "4": 0}
dictNaverTypes['E02'] = {"1": 0, "2": 0, "3": 0, "4": 0}
dictNaverTypes['E03'] = {"1": 0, "2": 0, "3": 0, "4": 0}
dictNaverTypes['E04'] = {"1": 0, "2": 0, "3": 0, "4": 0}
dictNaverTypes['F01'] = {"1": 0, "2": 0, "3": 0, "4": 0}
dictNaverTypes['Z00'] = {"1": 0, "2": 0, "3": 0, "4": 0}
dictNaverTypes['ETC'] = {"1": 0, "2": 0, "3": 0, "4": 0}

# listMasterTableFields Master Table 의 필드 이름 정리 - 네이버에서 추가 되는 필드 차단용
listMasterTableFields: list[str | Any] = ['seq', 'atclNo', 'masterCortarNo', 'masterCortarName', 'cortarNo', 'atclNm', 'atclStatCd', 'rletTpCd', 'rletTpNm', 'uprRletTpCd', 'tradTpCd',
                                          'tradTpNm', 'vrfcTpCd', 'flrInfo', 'prc', 'rentPrc', 'hanPrc', 'spc1', 'spc2', 'direction', 'atclCfmYmd', 'repImgUrl', 'repImgTpCd', 'repImgThumb',
                                          'lat', 'lng', 'atclFetrDesc',  'tagList', 'bildNm', 'minute', 'sameAddrCnt', 'sameAddrDirectCnt',
                                          'sameAddrHash', 'sameAddrMaxPrc',   'sameAddrMaxPrc2', 'sameAddrMinPrc', 'sameAddrMinPrc2', 'sameAddrPremMin', 'sameAddrPremMax', 'cpid',   'cpNm', 'cpCnt', 'rltrNm', 'sellrNm', 'directTradYn', 'minMviFee', 'maxMviFee', 'etRoomCnt',   'tradePriceHan', 'tradeRentPrice', 'tradeCheckedByOwner', 'cpLinkVO', 'dtlAddrYn', 'dtlAddr',   'point', 'state', 'modify_date', 'reg_date', 'tradeDayClusterNm', 'tradeYm']
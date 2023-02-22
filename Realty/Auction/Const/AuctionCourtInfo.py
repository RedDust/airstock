#initialization of constants

#법원 이름 정보
arrCourtName = { '서울중앙지방법원', '서울동부지방법원', '서울서부지방법원', '서울남부지방법원', '서울북부지방법원','의정부지방법원','고양지원','남양주지원','인천지방법원','부천지원','수원지방법원','성남지원','여주지원','평택지원','안산지원','안양지원'}

#물건 위치 정보
#41 : 서울
#11 : 경기도
#28 : 인천광역시
arrCityPlace = {'41', '11', '28'}


dictAuctionTypes = dict()
#상세물건조회
dictAuctionTypes['1'] = {'url': 'https://www.courtauction.go.kr/RetrieveRealEstMulDetailList.laf', 'type': '10'}
dictAuctionTypes['2'] = {'url': 'https://www.courtauction.go.kr/RetrieveMgakPlanMulSrch.laf', 'type': '20'}
dictAuctionTypes['3'] = {'url': 'https://www.courtauction.go.kr/RetrieveRealEstMgakGyulgwaMulList.laf', 'type': '30'}



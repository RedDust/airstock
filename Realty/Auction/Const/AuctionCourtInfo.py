#initialization of constants

#법원 이름 정보
arrCourtName = {'서울중앙지방법원', '서울동부지방법원', '서울서부지방법원', '서울남부지방법원', '서울북부지방법원', '의정부지방법원', '고양지원', '남양주지원', '인천지방법원', '부천지원', '수원지방법원', '성남지원', '여주지원', '평택지원', '안산지원', '안양지원'}


dictCourtInfo = dict()
dictCourtInfo['서울중앙지방법원'] = {"encoded": "%BC%AD%BF%EF%C1%DF%BE%D3%C1%F6%B9%E6%B9%FD%BF%F8"}
dictCourtInfo['서울동부지방법원'] = {"encoded": "%BC%AD%BF%EF%B5%BF%BA%CE%C1%F6%B9%E6%B9%FD%BF%F8"}
dictCourtInfo['서울서부지방법원'] = {"encoded": "%BC%AD%BF%EF%BC%AD%BA%CE%C1%F6%B9%E6%B9%FD%BF%F8"}
dictCourtInfo['서울남부지방법원'] = {"encoded": "%BC%AD%BF%EF%B3%B2%BA%CE%C1%F6%B9%E6%B9%FD%BF%F8"}
dictCourtInfo['서울북부지방법원'] = {"encoded": "%BC%AD%BF%EF%BA%CF%BA%CE%C1%F6%B9%E6%B9%FD%BF%F8"}
dictCourtInfo['의정부지방법원'] = {"encoded": "%C0%C7%C1%A4%BA%CE%C1%F6%B9%E6%B9%FD%BF%F8"}
dictCourtInfo['고양지원'] = {"encoded": "%B0%ED%BE%E7%C1%F6%BF%F8"}
dictCourtInfo['남양주지원'] = {"encoded": "%B3%B2%BE%E7%C1%D6%C1%F6%BF%F8"}
dictCourtInfo['인천지방법원'] = {"encoded": "%C0%CE%C3%B5%C1%F6%B9%E6%B9%FD%BF%F8"}
dictCourtInfo['부천지원'] = {"encoded": "%BA%CE%C3%B5%C1%F6%BF%F8"}
dictCourtInfo['수원지방법원'] = {"encoded": "%BC%F6%BF%F8%C1%F6%B9%E6%B9%FD%BF%F8"}
dictCourtInfo['성남지원'] = {"encoded": "%BC%BA%B3%B2%C1%F6%BF%F8"}
dictCourtInfo['여주지원'] = {"encoded": "%BF%A9%C1%D6%C1%F6%BF%F8"}
dictCourtInfo['평택지원'] = {"encoded": "%C6%F2%C5%C3%C1%F6%BF%F8"}
dictCourtInfo['안산지원'] = {"encoded": "%BE%C8%BB%EA%C1%F6%BF%F8"}
dictCourtInfo['안양지원'] = {"encoded": "%BE%C8%BE%E7%C1%F6%BF%F8"}



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



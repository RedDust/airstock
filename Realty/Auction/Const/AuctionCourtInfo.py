#initialization of constants

#법원 이름 정보
arrCourtName = {'서울중앙지방법원', '서울동부지방법원', '서울서부지방법원', '서울남부지방법원', '서울북부지방법원', '의정부지방법원', '고양지원',
                '남양주지원', '인천지방법원', '부천지원', '수원지방법원', '성남지원', '여주지원', '평택지원', '안산지원', '안양지원',
                '춘천지방법원' , '강릉지원', '원주지원', '속초지원','영월지원','청주지방법원','충주지원','제천지원','영동지원','대전지방법원',
                '홍성지원','논산지원','천안지원','공주지원','서산지원','대구지방법원','안동지원','경주지원','김천지원','상주지원','의성지원',
                '영덕지원','포항지원','대구서부지원','부산지방법원','부산동부지원','부산서부지원','울산지방법원','창원지방법원','마산지원',
                '진주지원','통영지원','밀양지원','거창지원','광주지방법원','목포지원','장흥지원','순천지원','해남지원','전주지방법원',
                '군산지원','정읍지원','남원지원','제주지방법원'
                }


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
#11 : 서울
#28 : 인천광역시
#26 : 부산광역시
#27 : 대구광역시
#29 : 광주광역시
#30 : 대전광역시
#31 : 울산광역시
#36 : 세종특별자치시
#41 : 경기도
#42 : 강원도
#43 : 충청북도
#44 : 충청남도
#45 : 전라북도
#46 : 전라남도
#47 : 경상북도
#48 : 경상남도
#50 : 제주특별자치도
#51 : 강원특별자치도

dictCityPlace = dict()
dictCityPlace['11'] = '서울특별시'
dictCityPlace['28'] = '인천광역시'
dictCityPlace['26'] = '부산광역시'
dictCityPlace['27'] = '대구광역시'
dictCityPlace['29'] = '광주광역시'
dictCityPlace['30'] = '대전광역시'
dictCityPlace['31'] = '울산광역시'
dictCityPlace['36'] = '세종특별자치시'
dictCityPlace['41'] = '경기도'
dictCityPlace['42'] = '강원도'
dictCityPlace['43'] = '충청북도'
dictCityPlace['44'] = '충청남도'
dictCityPlace['45'] = '전북'
dictCityPlace['46'] = '전라남도'
dictCityPlace['47'] = '경상북도'
dictCityPlace['48'] = '경상남도'
dictCityPlace['50'] = '제주특별자치도'
dictCityPlace['51'] = '강원특별자치도'


arrCityPlace = {'41', '11', '28','26','27','29','30','31','36','42','43','44','45','46','47','48','50','51'}


dictAuctionTypes = dict()
#상세물건조회
dictAuctionTypes['1'] = {'url': 'https://www.courtauction.go.kr/RetrieveRealEstMulDetailList.laf', 'type': '10'}
dictAuctionTypes['2'] = {'url': 'https://www.courtauction.go.kr/RetrieveMgakPlanMulSrch.laf', 'type': '20'}

dictAuctionCompleteTypes = dict()
dictAuctionCompleteTypes['url'] = 'https://www.courtauction.go.kr/RetrieveRealEstMgakGyulgwaMulList.laf'
dictAuctionCompleteTypes['type'] = '30'

dictBuildType = dict()
dictBuildType['00'] = '아파트'
dictBuildType['01'] = '오피스텔'
dictBuildType['02'] = '연립주택'
dictBuildType['03'] = '상가'
dictBuildType['04'] = '단독/다가구'
dictBuildType['05'] = '대지'
dictBuildType['06'] = '기타'
dictBuildType['07'] = '자동차/중기'


dictBuildTypeToCode = dict()
dictBuildTypeToCode['아파트'] = '00'
dictBuildTypeToCode['오피스텔'] = '01'
dictBuildTypeToCode['연립주택'] = '02'
dictBuildTypeToCode['상가'] = '03'
dictBuildTypeToCode['단독/다가구'] = '04'
dictBuildTypeToCode['대지'] = '05'
dictBuildTypeToCode['기타'] = '06'
dictBuildTypeToCode['자동차/중기'] = '07'


dictBuildTypeKeyWord = dict()
dictBuildTypeKeyWord['아파트'] = {'아파트'}
dictBuildTypeKeyWord['오피스텔'] = {'오피스텔'}
dictBuildTypeKeyWord['연립주택'] = {'연립주택', '빌라', '다세대', '연립주택다세대빌라'}
dictBuildTypeKeyWord['단독/다가구'] = {'단독주택', '다가구주택', '다가구', '단독주택다가구'}
dictBuildTypeKeyWord['상가'] = {'상가', '근린시설', '상가오피스텔근린시설'}
dictBuildTypeKeyWord['대지'] = {'대지', '임야', '전답', '대지임야전답'}
dictBuildTypeKeyWord['자동차/중기'] = {'자동차', '중기', '자동차중기'}
dictBuildTypeKeyWord['기타'] = {'기타'}




dictBuildTypeReverseKeyWord = dict()
dictBuildTypeReverseKeyWord['아파트'] = '아파트'

dictBuildTypeReverseKeyWord['오피스텔'] = '오피스텔'
dictBuildTypeReverseKeyWord['연립주택'] = '연립주택'
dictBuildTypeReverseKeyWord['빌라'] = '연립주택'
dictBuildTypeReverseKeyWord['다세대'] = '연립주택'
dictBuildTypeReverseKeyWord['연립주택다세대빌라'] = '연립주택'

dictBuildTypeReverseKeyWord['다가구주택'] = '단독/다가구'
dictBuildTypeReverseKeyWord['다가구'] = '단독/다가구'
dictBuildTypeReverseKeyWord['단독주택다가구'] = '단독/다가구'
dictBuildTypeReverseKeyWord['단독주택'] = '단독/다가구'

dictBuildTypeReverseKeyWord['상가'] = '상가'
dictBuildTypeReverseKeyWord['근린시설'] = '상가'
dictBuildTypeReverseKeyWord['상가오피스텔근린시설'] = '상가'

dictBuildTypeReverseKeyWord['대지'] = '대지'
dictBuildTypeReverseKeyWord['임야'] = '대지'
dictBuildTypeReverseKeyWord['전답'] = '대지'
dictBuildTypeReverseKeyWord['대지임야전답'] = '대지'

dictBuildTypeReverseKeyWord['자동차'] = '자동차/중기'
dictBuildTypeReverseKeyWord['중기'] = '자동차/중기'
dictBuildTypeReverseKeyWord['자동차중기'] = '자동차/중기'

dictBuildTypeReverseKeyWord['기타'] = '기타'




















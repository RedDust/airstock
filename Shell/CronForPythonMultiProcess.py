import os
import subprocess
import sys
import time
sys.path.append("D:/PythonProjects/airstock")
from multiprocessing import Process
from threading import Thread


import time
from datetime import datetime as DateTime, timedelta as TimeDelta
from multiprocessing import Process
from threading import Thread

dtNow = DateTime.today()

strBaseYYYY = str(dtNow.year).zfill(4)
strBaseMM = str(dtNow.month).zfill(2)
strBaseDD = str(dtNow.day).zfill(2)
strBaseHH = str(dtNow.hour).zfill(2)
strBaseII = str(dtNow.minute).zfill(2)
strBaseSS = str(dtNow.second).zfill(2)

intWeekDay = dtNow.weekday()
strNowTime = strBaseHH + ":" + strBaseII + ":" + strBaseSS



# from Realty.Auction import update_2_court_auction_geo_data_update_daily
# print("[Process START : " + strNowTime + "] update_2_court_auction_geo_data_update_daily")
# th1 = Thread(target=update_2_court_auction_geo_data_update_daily.main)
# # th1.daemon = True
# th1.start()
# print("[Process END : " + strNowTime + "] update_2_court_auction_geo_data_update_daily")
#
# from Realty.Auction import update_2_court_auction_complete_geo_data_update_daily
# print("[Process START : " + strNowTime + "] update_2_court_auction_complete_geo_data_update_daily")
# th1 = Thread(target=update_2_court_auction_complete_geo_data_update_daily.main)
# # th1.daemon = True
# th1.start()
# print("[Process END : " + strNowTime + "] update_2_court_auction_complete_geo_data_update_daily")


#1년에 1회 백업
if strBaseMM == "01" and strBaseDD == "05":
    if strBaseHH == "12" and strBaseII == "00":

        # #[035301] 국토교통부 데이터 백업
        from Realty.Government import backup_3_korea_townhouse_trade_data_backup
        th1 = Thread(target=backup_3_korea_townhouse_trade_data_backup.main)
        # th1.daemon = True
        th1.start()

#1년에 1회 새해 테이블 생성
if strBaseMM == "12" and strBaseDD == "15":
    if strBaseHH == "12" and strBaseII == "00":
        # 연 / 월 테이블 생성
        from Realty.DatabaseWork import create_tables_for_year
        th1 = Thread(target=create_tables_for_year.main)
        # th1.daemon = True
        th1.start()



#매주 일요일
if intWeekDay == 6:
    if strBaseHH == "12" :
        if strBaseII == "00":

            # #[041101] 어린이집별 기본정보 조회
            from Realty.Government import get_1_children_daycare_center_data
            th1 = Thread(target=get_1_children_daycare_center_data.main)
            # th1.daemon = True
            th1.start()


    # if strBaseHH == "00":
    #     if strBaseII == "00":




if strBaseHH == "00":
    if strBaseII == "00":

        from Shell import DeleteLog
        strLogPath = 'D:/PythonProjects/airstock/Shell/logs/'
        DeleteLog.main(strLogPath)



    if strBaseII == "10":
        #[035111] 국토교통부_아파트 부동산 매매 신고 자료
        from Realty.Government import get_1_korea_trade_data_daily
        th1 = Thread(target=get_1_korea_trade_data_daily.main)
        # th1.daemon = True
        th1.start()


    if strBaseII == "20":
        # # [034180] 지역 데이터 - 서울(41) 지하철 사용량(80)
        from Realty.Government import get_1_seoul_subway_using_data_daily
        print("[Process START : " + strNowTime + "] get_1_seoul_subway_using_data_daily")
        th1 = Thread(target=get_1_seoul_subway_using_data_daily.main)
        # th1.daemon = True
        th1.start()
        print("[Process END : " + strNowTime + "] get_1_seoul_subway_using_data_daily")

    if strBaseII == "30":
        # # [034181] 지역 데이터 - 서울(41) 지하철 Geo 정보(81)
        from Realty.Government import get_1_seoul_subway_station_geo_data
        print("[Process START : " + strNowTime + "] get_1_seoul_subway_station_geo_data")
        th1 = Thread(target=get_1_seoul_subway_station_geo_data.main)
        # th1.daemon = True
        th1.start()
        print("[Process END : " + strNowTime + "] get_1_seoul_subway_station_geo_data")



    #     # #[041101] 어린이집별 기본정보 조회
    #     from Realty.Government import get_1_children_daycare_center_data
    #     th1 = Thread(target=get_1_children_daycare_center_data.main)
    #     # th1.daemon = True
    #     th1.start()



    if strBaseII == "50":

        # #[035121] 국토교통부 단독/다가구 매매 실거래가
        from Realty.Government import get_1_korea_house_trade_data_daily
        th1 = Thread(target=get_1_korea_house_trade_data_daily.main)
        # th1.daemon = True
        th1.start()


elif strBaseHH == "01":

    if strBaseII == "00":
        # #[035141] 국토교통부_오피스텔 매매 실거래 자료
        from Realty.Government import get_1_korea_officetel_trade_data_daily
        th1 = Thread(target=get_1_korea_officetel_trade_data_daily.main)
        # th1.daemon = True
        th1.start()


    if strBaseII == "10":
        #[035161] 국토교통부 토지 매매 실거래가
        from Realty.Government import get_1_korea_land_trade_data_daily
        print("[Process START : " + strNowTime + "] get_1_korea_land_trade_data_daily")
        th1 = Thread(target=get_1_korea_land_trade_data_daily.main)
        # th1.daemon = True
        th1.start()
        print("[Process END : " + strNowTime + "] get_1_korea_land_trade_data_daily")

    if strBaseII == "20":
        #[035151] 국토교통부_상업업무용 부동산 매매 신고 자료
        from Realty.Government import get_1_korea_business_trade_data_daily
        print("[Process START : " + strNowTime + "] get_1_korea_business_trade_data_daily")
        th1 = Thread(target=get_1_korea_business_trade_data_daily.main)
        # th1.daemon = True
        th1.start()
        print("[Process END : " + strNowTime + "] get_1_korea_business_trade_data_daily")

    if strBaseII == "30":
        #[035131] 국토교통부_연립/다세대 매매 실거래 자료
        from Realty.Government import get_1_korea_townhouse_trade_data_daily
        print("[Process START : " + strNowTime + "] get_1_korea_townhouse_trade_data_daily")
        th1 = Thread(target=get_1_korea_townhouse_trade_data_daily.main)
        # th1.daemon = True
        th1.start()
        print("[Process END : " + strNowTime + "] get_1_korea_townhouse_trade_data_daily")

    if strBaseII == "40":
        #[035221] 국토교통부 단독/다가구 임대차 실거래가
        from Realty.Government import get_1_korea_house_rent_data_daily
        print("[Process START : " + strNowTime + "] get_1_korea_house_rent_data_daily")
        th1 = Thread(target=get_1_korea_house_rent_data_daily.main)
        # th1.daemon = True
        th1.start()
        print("[Process END : " + strNowTime + "] get_1_korea_house_rent_data_daily")

    if strBaseII == "50":
        #[035231] 국토교통부 연립주택 임대차 실거래가
        from Realty.Government import get_1_korea_townhouse_rent_data_daily
        print("[Process START : " + strNowTime + "] get_1_korea_townhouse_rent_data_daily")
        th1 = Thread(target=get_1_korea_townhouse_rent_data_daily.main)
        # th1.daemon = True
        th1.start()
        print("[Process END : " + strNowTime + "] get_1_korea_townhouse_rent_data_daily")


if strBaseHH == "02":

    if strBaseII == "00":
        #[035211] 국토교통부_아파트 임대차 신고 자료
        from Realty.Government import get_1_korea_apt_rent_data_daily
        print("[Process START : " + strNowTime + "] get_1_korea_apt_rent_data_daily")
        th1 = Thread(target=get_1_korea_apt_rent_data_daily.main)
        # th1.daemon = True
        th1.start()
        print("[Process END : " + strNowTime + "] get_1_korea_apt_rent_data_daily")

    if strBaseII == "10":
        print("[Process START : " + strNowTime + "] strBaseHH")
        # [040101] 날씨 정보 수집
        print("[Process START : " + strNowTime + "] get_1_weather_daily")
        from Realty.Government import get_1_weather_daily
        th1 = Thread(target=get_1_weather_daily.main)
        # th1.daemon = True
        th1.start()
        print("[Process END : " + strNowTime + "] get_1_weather_daily")

    if strBaseII == "20":
        # #[035241] 국토교통부 오피스텔 임대차 실거래가
        from Realty.Government import get_1_korea_officetel_rent_data_daily
        print("[Process START : " + strNowTime + "] get_1_korea_officetel_rent_data_daily")
        th1 = Thread(target=get_1_korea_officetel_rent_data_daily.main)
        # th1.daemon = True
        th1.start()
        print("[Process END : " + strNowTime + "] get_1_korea_officetel_rent_data_daily")

    if strBaseII == "40":
        #[034190]지역 실거래 데이터 - 서울(41) 시설공사(90)
        from Realty.Government import get_1_seoul_sisul_data
        print("[Process START : " + strNowTime + "] get_1_seoul_sisul_data")
        th1 = Thread(target=get_1_seoul_sisul_data.main)
        # th1.daemon = True
        th1.start()
        print("[Process END : " + strNowTime + "] get_1_seoul_sisul_data")


if strBaseHH == "03":
    if strBaseII == "00":
        print("[Process START : " + strNowTime + "] strBaseHH")

        # [034175] 지역 실거래 데이터 - 서울(41) 버스 정류장 위도경도 데이터 (75)
        from Realty.Government import get_1_seoul_bus_stop_geo_data
        print("[Process START : " + strNowTime + "] get_1_seoul_bus_stop_geo_data")
        th1 = Thread(target=get_1_seoul_bus_stop_geo_data.main)
        # th1.daemon = True
        th1.start()
        print("[Process END : " + strNowTime + "] get_1_seoul_bus_stop_geo_data")

    if strBaseII == "10":
        print("[Process START : " + strNowTime + "] strBaseHH")

        # # [034176] 지역 실거래 데이터 - 서울(41) 버스 정류장 위도경도 주소 UPDATE(76)
        # from Realty.Government import update_2_seoul_bus_stop_geo_data
        # print("[Process START : " + strNowTime + "] update_2_seoul_bus_stop_geo_data")
        # th1 = Thread(target=update_2_seoul_bus_stop_geo_data.strGeoUpdate)
        # # th1.daemon = True
        # th1.start()
        # print("[Process END : " + strNowTime + "] update_2_seoul_bus_stop_geo_data")

    if strBaseII == "20":
        print("[Process START : " + strNowTime + "] strBaseHH")

        # [034176] 지역 실거래 데이터 - 서울(41) 버스 정류장 승하차 인원 데이터 UPDATE(77)
        from Realty.Government import update_2_seoul_bus_stop_rank

        print("[Process START : " + strNowTime + "] update_2_seoul_bus_stop_rank")
        th1 = Thread(target=update_2_seoul_bus_stop_rank.main)
        # th1.daemon = True
        th1.start()
        print("[Process END : " + strNowTime + "] update_2_seoul_bus_stop_rank")

        # #[035112] 국토교통부 매매 거래 통계 작성
        from Realty.Government import make_2_molit_trade_daily_statistics
        print("[Process START : " + strNowTime + "] make_2_molit_trade_daily_statistics")
        # th1 = Thread(target=make_2_molit_trade_daily_statistics.main)
        # # th1.daemon = True
        # th1.start()
        # print("[Process END : " + strNowTime + "] make_2_molit_trade_daily_statistics")

        # #[035212] 국토교통부 임대차 거래 통계 작성
        # from Realty.Government import make_2_molit_apt_rent_daily_statistics
        # print("[Process START : " + strNowTime + "] make_2_molit_apt_rent_daily_statistics")
        # th1 = Thread(target=make_2_molit_apt_rent_daily_statistics.main)
        # # th1.daemon = True
        # th1.start()
        # print("[Process END : " + strNowTime + "] make_2_molit_apt_rent_daily_statistics")



if strBaseHH == "04":
    if strBaseII == "00":
        print("[Process START : " + strNowTime + "] strBaseHH")

        # [034160] 서울시 공동주택 아파트 정보
        from Realty.Government import get_1_seoul_apt_info_data_daily
        print("[Process START : " + strNowTime + "] get_1_seoul_apt_info_data_daily")
        th1 = Thread(target=get_1_seoul_apt_info_data_daily.main)
        # th1.daemon = True
        th1.start()
        print("[Process END : " + strNowTime + "] get_1_seoul_apt_info_data_daily")

    if strBaseII == "10":
        print("[Process START : " + strNowTime + "] strBaseHH")
        # [034110] 서울 실거래 매매 데이터 통계 작성
        from Realty.Government import make_2_trade_statistics_data
        print("[Process START : " + strNowTime + "] make_2_trade_statistics_data")
        th1 = Thread(target=make_2_trade_statistics_data.main)
        # th1.daemon = True
        th1.start()
        print("[Process END : " + strNowTime + "] make_2_trade_statistics_data")

    if strBaseII == "30":
        #[034210] 서울 실거래 렌트 데이터 통계 작성
        print("[Process START : " + strNowTime + "] make_2_rent_statistics_data")
        from Realty.Government import make_2_rent_statistics_data
        th1 = Thread(target=make_2_rent_statistics_data.main)
        # th1.daemon = True
        th1.start()
        print("[Process END : " + strNowTime + "] make_2_rent_statistics_data")


    if strBaseII == "50":
        # #[035113] 국토교통부 아파트 매매 거래 통계 작성(연간)
        from Realty.Government import make_2_molit_apt_trade_statistics_year
        print("[Process START : " + strNowTime + "] make_2_molit_apt_trade_statistics_year")
        th1 = Thread(target=make_2_molit_apt_trade_statistics_year.main)
        # th1.daemon = True
        th1.start()
        print("[Process END : " + strNowTime + "] make_2_molit_apt_trade_statistics_year")




elif strBaseHH == "05":
    if strBaseII == "00":
        print("[Process START : " + strNowTime + "] strBaseHH")
        # [040101] 날씨 정보 수집
        from Realty.Government import get_1_weather_daily
        print("[Process START : " + strNowTime + "] get_1_weather_daily")
        th1 = Thread(target=get_1_weather_daily.main)
        # th1.daemon = True
        th1.start()
        print("[Process END : " + strNowTime + "] get_1_weather_daily")

    if strBaseII == "10":
        print("[Process START : " + strNowTime + "] strBaseHH")
        #[011010] 네이버 물건 월별 백업 및 마스터 삭제
        from Realty.Naver import backup_2_rand_data_for_month
        print("[Process START : " + strNowTime + "] backup_2_rand_data_for_month")
        th1 = Thread(target=backup_2_rand_data_for_month.main)
        # th1.daemon = True
        th1.start()
        print("[Process END : " + strNowTime + "] backup_2_rand_data_for_month")

    if strBaseII == "50":
        print("[Process START : " + strNowTime + "] strBaseHH")
        #[021000] 법원 경매 부동산 데이터 백업
        # from Realty.Auction import backup_3_court_auction_daily
        # print("[Process START : " + strNowTime + "] backup_3_court_auction_daily")
        # th1 = Thread(target=backup_3_court_auction_daily.main)
        # # th1.daemon = True
        # th1.start()
        # print("[Process END : " + strNowTime + "] backup_3_court_auction_daily")



elif strBaseHH == "06":

    if strBaseII == "00":
        print("[Process START : " + strNowTime + "] strBaseHH")


    if strBaseII == "10":
        # 06시 10분
        #[020000] 법원 경매 부동산 물건 수집(공통) -추후 지역 분할 가능
        from Realty.Auction import get_1_acution_data
        print("[Process START : " + strNowTime + "] get_1_acution_data")
        th1 = Thread(target=get_1_acution_data.main)
        # th1.daemon = True
        th1.start()
        print("[Process END : " + strNowTime + "] get_1_acution_data")



    if strBaseII == "30":
        print("[Process START : " + strNowTime + "] strBaseHH")
        # [022001] 법원경매 법정 시도 정보 수집
        from Realty.Auction import get_1_auction_sido_info
        print("[Process START : " + strNowTime + "] get_1_auction_sido_info")
        th1 = Thread(target=get_1_auction_sido_info.main)
        # th1.daemon = True
        th1.start()
        print("[Process END : " + strNowTime + "] get_1_auction_sido_info")

    if strBaseII == "40":
        print("[Process START : " + strNowTime + "] strBaseHH")
        # [022002] 법원경매 법정 시군구 정보 수집
        # from Realty.Auction import get_2_auction_sigu_info
        # print("[Process START : " + strNowTime + "] get_2_auction_sigu_info")
        # th1 = Thread(target=get_2_auction_sigu_info.main)
        # # th1.daemon = True
        # th1.start()
        # print("[Process END : " + strNowTime + "] get_2_auction_sigu_info")


elif strBaseHH == "07":

    if strBaseII == "10":
        print("[Process START : " + strNowTime + "] strBaseHH")

        # [020001] 법원경매 물건수집 - (예정)
        from Realty.Auction import get_1_auction_data_planned
        print("[Process START : " + strNowTime + "] get_1_auction_data_planned.py")
        # th1 = Thread(target=get_1_auction_planned_data.main)
        # # th1.daemon = True
        # th1.start()
        # print("[Process END : " + strNowTime + "] get_1_auction_data_planned.py")





elif strBaseHH == "08":


    if strBaseII == "20":
        print("[Process START : " + strNowTime + "] strBaseHH")

        #[012000] 네이버 부동산 일별 통계 데이터 작성
        from Realty.Naver import make_2_daily_statistics_data
        print("[Process START : " + strNowTime + "] make_2_daily_statistics_data")
        th1 = Thread(target=make_2_daily_statistics_data.main)
        # th1.daemon = True
        th1.start()
        print("[Process END : " + strNowTime + "] make_2_daily_statistics_data")

        # 08시 00분



elif strBaseHH == "10":

    if strBaseII == "00":
        print("[Process START : " + strNowTime + "] strBaseHH")

        # [020001] 법원경매 물건수집 - (예정)
        from Realty.Auction import get_1_auction_data_planned
        print("[Process START : " + strNowTime + "] get_1_auction_data_planned.py")
        th1 = Thread(target=get_1_auction_data_planned.main)
        # th1.daemon = True
        th1.start()
        print("[Process END : " + strNowTime + "] get_1_auction_data_planned.py")




elif strBaseHH == "11":
    if strBaseII == "00":
        print("[Process START : " + strNowTime + "] strBaseHH")
        from Realty.Auction import get_1_auction_data_complete
        print("[Process START : " + strNowTime + "] get_1_auction_data_complete.py")
        th1 = Thread(target=get_1_auction_data_complete.main)
        # th1.daemon = True
        th1.start()
        print("[Process END : " + strNowTime + "] get_1_auction_data_complete.py")


    if strBaseII == "20":
        print("[Process START : " + strNowTime + "] strBaseHH")

    if strBaseII == "30":
        print("[Process START : " + strNowTime + "] strBaseHH")
        # [020101] 법원 경매 부동산 MASTER 테이블 위도경도 업데이트
        from Realty.Auction import update_2_court_auction_geo_data_update_daily
        print("[Process START : " + strNowTime + "] update_2_court_auction_geo_data_update_daily")
        th1 = Thread(target=update_2_court_auction_geo_data_update_daily.main)
        # th1.daemon = True
        th1.start()
        print("[Process END : " + strNowTime + "] update_2_court_auction_geo_data_update_daily")






elif strBaseHH == "13":
    if strBaseII == "00":
        print("[Process START : " + strNowTime + "] strBaseHH")

    if strBaseII == "40":
        print("[Process START : " + strNowTime + "] strBaseHH")
        #[020102] 법원 경매 부동산 COMPLETE 테이블 위도경도 업데이트
        from Realty.Auction import update_2_court_auction_complete_geo_data_update_daily
        print("[Process START : " + strNowTime + "] update_2_court_auction_complete_geo_data_update_daily")
        th1 = Thread(target=update_2_court_auction_complete_geo_data_update_daily.main)
        # th1.daemon = True
        th1.start()
        print("[Process END : " + strNowTime + "] update_2_court_auction_complete_geo_data_update_daily")


    if strBaseII == "50":
        # [020020] 법원경매 통계 데이터 작성
        from Realty.Auction import update_3_daily_stastistics
        print("[Process START : " + strNowTime + "] update_3_daily_stastistics")
        th1 = Thread(target=update_3_daily_stastistics.main)
        # th1.daemon = True
        th1.start()
        print("[Process END : " + strNowTime + "] update_3_daily_stastistics")


elif strBaseHH == "15":
    if strBaseII == "00":
        print("[Process START : " + strNowTime + "] strBaseHH")


elif strBaseHH == "16":
    if strBaseII == "00":
        print("[Process START : " + strNowTime + "] strBaseHH")
        # [040101] 날씨 정보 수집
        from Realty.Government import get_1_weather_daily
        print("[Process START : " + strNowTime + "] get_1_weather_daily")
        th1 = Thread(target=get_1_weather_daily.main)
        # th1.daemon = True
        th1.start()
        print("[Process END : " + strNowTime + "] get_1_weather_daily")

    if strBaseII == "20":
        print("[Process START : " + strNowTime + "] strBaseHH")
        # [042101] 법정동 API 수집
        from Realty.Government import get_1_korea_gov_code_info_api
        print("[Process START : " + strNowTime + "] get_1_korea_gov_code_info_api")
        th1 = Thread(target=get_1_korea_gov_code_info_api.main)
        # th1.daemon = True
        th1.start()
        print("[Process END : " + strNowTime + "] get_1_korea_gov_code_info_api")


    if strBaseII == "30":
        print("[Process START : " + strNowTime + "] strBaseHH")





elif strBaseHH == "18":
    if strBaseII == "00":
        print("[Process START : " + strNowTime + "] strBaseHH")
        # [040101] 날씨 정보 수집
        from Realty.Government import get_1_weather_daily
        print("[Process START : " + strNowTime + "] get_1_weather_daily")
        th1 = Thread(target=get_1_weather_daily.main)
        # th1.daemon = True
        th1.start()
        print("[Process END : " + strNowTime + "] get_1_weather_daily")

elif strBaseHH == "20":
    if strBaseII == "00":
        print("[Process START : " + strNowTime + "] strBaseHH")
        # [040101] 날씨 정보 수집
        from Realty.Government import get_1_weather_daily
        print("[Process START : " + strNowTime + "] get_1_weather_daily")
        th1 = Thread(target=get_1_weather_daily.main)
        # th1.daemon = True
        th1.start()
        print("[Process END : " + strNowTime + "] get_1_weather_daily")

elif strBaseHH == "22":
    if strBaseII == "00":
        print("[Process START : " + strNowTime + "] strBaseHH")
        # [040101] 날씨 정보 수집
        print("[Process START : " + strNowTime + "] get_1_weather_daily")
        th1 = Thread(target=get_1_weather_daily.main)
        # th1.daemon = True
        th1.start()
        print("[Process END : " + strNowTime + "] get_1_weather_daily")


print("[CRONTAB END : "+strNowTime+"]=====================================")

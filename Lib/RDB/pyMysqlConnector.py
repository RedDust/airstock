import pymysql

def ResKtRealEstateConnection():
    conn = pymysql.connect(host='localhost', user='kt_real_estate_auction', port=3366,
                       password='rhwocns!@84', db='kt_real_estate_auction', charset='utf8')

    return conn
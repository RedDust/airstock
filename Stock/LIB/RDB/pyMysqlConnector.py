import pymysql

def ResKtRealEstateConnection():
    conn = pymysql.connect(host='localhost', user='kt_real_estate_auction', port=3366,
                       password='rhwocns!@84', db='kt_real_estate_auction', charset='utf8')

    return conn


def goDaqConnection():
    conn = pymysql.connect(host='localhost', user='root', port=3366,
                       password='rhwocns!@84', db='goDaq', charset='utf8')

    return conn


def StockFriendsConnection():
    conn = pymysql.connect(host='localhost', user='root', port=3366,
                       password='rhwocns!@84', db='stockfriends', charset='utf8')

    return conn


def KoreaInvestItemsConnection():
    conn = pymysql.connect(host='localhost', user='root', port=3366,
                       password='rhwocns!@84', db='koreainvest_items', charset='utf8')

    return conn

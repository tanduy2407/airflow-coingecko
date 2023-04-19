import requests
import pandas as pd
import mysql.connector
import time
import pyspark


list_coin = ['bitcoin', 'ethereum', 'binancecoin', 'ripple',
             'cardano', 'dogecoin', 'matic-network', 'solana']


def connect_mysql():
    conn = mysql.connector.connect(host='localhost',
                                   database='coingecko',
                                   user='root',
                                   password='tanduy2407')
    return conn


def query_to_db(conn, keyword, table, data):
    cur = conn.cursor()
    if keyword == 'create_table':
        sql = f"""
		CREATE TABLE IF NOT EXISTS {table}(
					id VARCHAR(20),
					name VARCHAR(50),
					symbol VARCHAR(10),
					description VARCHAR(500),
					homepage VARCHAR(50),
					genesis_date VARCHAR(10),
					ath FLOAT,
					ath_date DATE,
					atl FLOAT,
					atl_date DATE)"""
        cur.execute(sql)

        sql = f"""CREATE TABLE IF NOT EXISTS {table}(
					id VARCHAR(20),
					name VARCHAR(50),
					date DATE,
					price FLOAT)"""
        cur.execute(sql)
        conn.close()
    if keyword == 'read':
        sql = f'SELECT * FROM {table}'
        df = pd.read_sql_query(sql, conn)
        conn.close()
        return df

    if keyword == 'insert':
        sql = ''
        if table == 'basic_infomation':
            sql = f'INSERT INTO {table} VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'

        if table == 'historical_data':
            sql = f'INSERT INTO {table} VALUES (%s, %s, %s, %s)'

        cur.executemany(sql, data)
        conn.commit()
        print(cur.rowcount, 'record inserted')


def etl_general_info(conn):
    values = []
    for coin in list_coin:
        res = requests.get(
            f'https://api.coingecko.com/api/v3/coins/{coin}').json()
        id = res['id']
        name = res['name']
        symbol = res['symbol']
        description = '.'.join(res['description']['en'].split('.')[:3])
        homepage = res['links']['homepage'][0]
        genesis_date = res['genesis_date']
        ath = res['market_data']['ath']['usd']
        ath_date = res['market_data']['ath_date']['usd'][:10]
        atl = res['market_data']['atl']['usd']
        atl_date = res['market_data']['atl_date']['usd'][:10]
        info = (id, name, symbol, description, homepage,
                genesis_date, ath, ath_date, atl, atl_date)
        values.append(info)
        query_to_db(conn, keyword='insert',
                    table='basic_infomation', data=values)
    conn.close()


def etl_historical_info(conn):
    start_date = '2023-04-14'
    end_date = '2023-04-18'
    dates = pd.date_range(start_date, end_date)
    for coin in list_coin:
        values = []
        for date in dates:
            date_param = date.strftime('%d-%m-%Y')
            print('Insert data on %s of %s' % (date_param, coin))
            res = requests.get(
                f'https://api.coingecko.com/api/v3/coins/{coin}/history?date={date_param}')
            print(res.status_code)
            if res.status_code == 200:
                pass
            elif res.status_code == 429:
                time.sleep(65)
                res = requests.get(
                    f'https://api.coingecko.com/api/v3/coins/{coin}/history?date={date_param}')
                print(res.status_code)
            request = res.json()
            id = request['id']
            name = request['name']
            price = request['market_data']['current_price']['usd']
            date = str(date)[:10]
            info = (id, name, date, price)
            values.append(info)
        print(values)
        query_to_db(conn, keyword='insert',
                    table='historical_data', data=values)


if __name__ == "__main__":
    # etl_info()
    conn = connect_mysql()
    etl_historical_info(conn)

import psycopg2
import datetime
import base64


def upload_clients_hist():
    #словарь для хранения и обработки данных по клиентам
    rep_clients = {}
    #Подключаемся к бд
    read_conn = psycopg2.connect(dbname='dwh', user='dwh_krasnoyarsk',
                                 password=base64.b64decode('ZHdoX2tyYXNub3lhcnNrX3VCUGFYTlN4').decode('utf-8'), host='de-edu-db.chronosavant.ru')
    read_cursor = read_conn.cursor()
    # получаем максимальное start_dt - время когда мы последний раз обновляли данные и берем все что изменилось после него
    read_cursor.execute(f"SELECT MAX(start_dt) FROM rep_clients_hist;")
    max_update_dt = read_cursor.fetchall()
    if max_update_dt == [(None,)]:
        max_update_dt = datetime.datetime(2004, 9, 29)
    else:
        max_update_dt = max_update_dt[0][0]
    #Считываем данные о существующих клиентах и заполняем словарь для дальнейшей работы
    read_cursor.execute(f"SELECT phone_num, deleted_flag FROM dim_clients")
    clients_readed = read_cursor.fetchall()
    for i in clients_readed:
        rep_clients[i[0]] = {'rides_cnt': 0, 'cancelled_cnt': 0, 'spent_amt': 0, 'debt_amt': 0, 'deleted_flag': i[1]}

    #Считываем данные из склеенных таблиц для того, чтобы найти суммы оплат, долга и поездок
    read_cursor.execute(f"SELECT client_phone_num, ride_start_dt, prices, transactions FROM fact_rides"
                        f" INNER JOIN (SELECT phone_num, transactions, prices, prices-transactions FROM"
                        f" (SELECT phone_num, SUM(transaction_amt) AS transactions  from dim_clients INNER JOIN fact_payments ON REPLACE(dim_clients.card_num, ' ', '') = fact_payments.card_num GROUP BY phone_num) trans"
                        f" INNER JOIN (SELECT phone_num, SUM(price_amt) AS prices from dim_clients INNER JOIN fact_rides ON dim_clients.phone_num = fact_rides.client_phone_num WHERE fact_rides.ride_start_dt is not Null GROUP BY phone_num) debt"
                        f" USING (phone_num)) money ON fact_rides.client_phone_num = money.phone_num"
                        f" ")
    rides_readed = read_cursor.fetchall()

    #Распределяем полученные данные по словарю
    for i in rides_readed:
        rep_clients[i[0]]['spent_amt'] = i[3]
        rep_clients[i[0]]['debt_amt'] = i[2]-i[3]
        if i[1] == None:
            rep_clients[i[0]]['cancelled_cnt'] += 1
        else:
            rep_clients[i[0]]['rides_cnt'] += 1

    #Записываем данные в таблицу
    for i in rep_clients:
        read_cursor.execute(f"SELECT * FROM rep_clients_hist WHERE phone_num = '{i}'")
        clients_from_hist = read_cursor.fetchall()
        if not clients_from_hist:
            read_cursor.execute("INSERT INTO rep_clients_hist (phone_num, rides_cnt, cancelled_cnt, spent_amt, debt_amt, start_dt, end_dt, deleted_flag) VALUES(%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s, %s)",
                                (i, rep_clients[i]['rides_cnt'], rep_clients[i]['cancelled_cnt'],
                                 rep_clients[i]['spent_amt'], rep_clients[i]['debt_amt'],
                                 datetime.datetime(9999, 12, 31), rep_clients[i]['deleted_flag']))
        elif list(rep_clients[i].values()) != list(clients_from_hist[0]):
            read_cursor.execute(f"UPDATE rep_clients_hist SET end_dt = CURRENT_TIMESTAMP - INTERVAL '1 second'"
                                f" WHERE phone_num = '{i}'"
                                f" AND start_dt = (SELECT MAX(start_dt) FROM rep_clients_hist WHERE phone_num = '{i}')")
            read_cursor.execute("INSERT INTO rep_clients_hist (phone_num, rides_cnt, cancelled_cnt, spent_amt, debt_amt, start_dt, end_dt, deleted_flag) VALUES( %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s, %s)",
                                (i, rep_clients[i]['rides_cnt'], rep_clients[i]['cancelled_cnt'],
                                 rep_clients[i]['spent_amt'], rep_clients[i]['debt_amt'],
                                 datetime.datetime(9999, 12, 31), rep_clients[i]['deleted_flag']))
        read_conn.commit()

    read_cursor.close()
    read_conn.close()
    return "OK"
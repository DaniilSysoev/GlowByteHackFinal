import datetime
import psycopg2
import decimal
import base64


def update_drivers_payments():
    #Подключаемся к созданной табличке
    read_conn = psycopg2.connect(dbname='dwh', user='dwh_krasnoyarsk', 
                            password=base64.b64decode('ZHdoX2tyYXNub3lhcnNrX3VCUGFYTlN4').decode('utf-8'), host='de-edu-db.chronosavant.ru')
    read_cursor = read_conn.cursor()

    # получаем максимальное start_dt - время когда мы последний раз обновляли данные и берем все что изменилось после него
    read_cursor.execute(f"SELECT MAX(report_dt) FROM rep_drivers_payments;")
    max_update_dt = read_cursor.fetchall()
    if max_update_dt == [(None,)]:
        max_update_dt = datetime.datetime(2004, 9, 29)
    else:
        max_update_dt = max_update_dt[0][0]

    #Считываем табличку dim_drivers для распределения по водителям
    read_cursor.execute(f"SELECT * FROM dim_drivers WHERE start_dt > '{max_update_dt}'")
    drivers_readed = read_cursor.fetchall()
    #Распределение по персональным номерам
    drivers = {}
    for i in drivers_readed:
        drivers[i[0]] = [i[2], i[3], i[4], i[6].replace(" ", ""), []] #"report_dt" "price_amt" "transaction_amt" "distance_val"
    drivers_dates = {}

    #Считываем табличку fact_rides для распределения по датам и получения суммы цен
    read_cursor.execute(f"SELECT price_amt, driver_pers_num, ride_end_dt, distance_val, client_phone_num from fact_rides where ride_start_dt is not Null AND ride_end_dt > '{max_update_dt}'")
    ride_readed = read_cursor.fetchall()
    ride = []
    for i in range(len(ride_readed)):
        if ride_readed[i][1].replace(" ", "") != "-1":
            ride.append((ride_readed[i][0], int(ride_readed[i][1].replace(" ", "")), ride_readed[i][2].date(), ride_readed[i][3], ride_readed[i][4]))
    ride.sort(key=lambda x: x[1])
    ride_drivers_num = []
    ride_clients_num = []
    for i in ride:
        ride_drivers_num.append(i[1])
        ride_clients_num.append(i[4])
    #Создаем словарь для того, чтобы контролировать даты, которые мы уже обработали, и вносим данные в словарь drivers
    was_dt = {}
    for i in range(len(ride)):
        if not drivers[ride[i][1]][4]:
            drivers[ride[i][1]][4].append([ride[i][2], ride[i][0], 0, ride[i][3]])
            was_dt[ride[i][1]] = [ride[i][2]]
        else:
            if ride[i][2] in was_dt[ride[i][1]]:
                drivers[ride[i][1]][4][was_dt[ride[i][1]].index(ride[i][2])][1] += ride[i][0]
                drivers[ride[i][1]][4][was_dt[ride[i][1]].index(ride[i][2])][3] += ride[i][3]
            else:
                drivers[ride[i][1]][4].append([ride[i][2], ride[i][0], 0, ride[i][3]])
                was_dt[ride[i][1]].append(ride[i][2])
    for i in drivers:
        drivers_dates[i] = [drivers[i][4][j][0] for j in range(len(drivers[i][4]))]

    #Запись в новую таблицу
    write_conn = psycopg2.connect(dbname='dwh', user='dwh_krasnoyarsk',
                                  password='dwh_krasnoyarsk_uBPaXNSx', host='de-edu-db.chronosavant.ru')
    write_cursor = write_conn.cursor()
    write_cursor.execute("SELECT personnel_num, report_dt FROM rep_drivers_payments")
    #Считывание имеющихся данных, чтобы избежать повторений
    dates = {}
    nums = []
    for i in write_cursor.fetchall():
        if i[0] in nums:
            dates[i[0]].append(i[1])
        else:
            dates[i[0]] = [i[1]]
            nums.append(i[0])
    for i in drivers:
        #Проверяем наличие водителя в бд
        if i in nums:
            for j in range(len(drivers[i][4])):
                #Проверяем наличие даты для выбранного водителя, и если такая дата имеется, то перезаписываем amount
                if drivers[i][4][j][0] in dates[i]:
                    write_cursor.execute(f"UPDATE rep_drivers_payments SET amount = '{drivers[i][4][j][1]-(decimal.Decimal(0.2)*drivers[i][4][j][1] + decimal.Decimal(47.26*7)*drivers[i][4][j][3]/decimal.Decimal(100) + decimal.Decimal(5)*drivers[i][4][j][3])}'"
                                         f" WHERE personnel_num = '{i}' AND report_dt = '{drivers[i][4][j][0]}'")
                else:
                    write_cursor.execute(f"INSERT INTO rep_drivers_payments VALUES ('{i}', '{drivers[i][0]}', '{drivers[i][1]}', '{drivers[i][2]}', '{drivers[i][3]}', '{drivers[i][4][j][1]-(decimal.Decimal(0.2)*drivers[i][4][j][1] + decimal.Decimal(47.26*7)*drivers[i][4][j][3]/decimal.Decimal(100) + decimal.Decimal(5)*drivers[i][4][j][3])}', '{drivers[i][4][j][0]}')")
        else:
            for j in range(len(drivers[i][4])):
                write_cursor.execute(
                    f"INSERT INTO rep_drivers_payments VALUES ('{i}', '{drivers[i][0]}', '{drivers[i][1]}', '{drivers[i][2]}', '{drivers[i][3]}', '{drivers[i][4][j][1] - (decimal.Decimal(0.2) * drivers[i][4][j][1] + decimal.Decimal(47.26 * 7) * drivers[i][4][j][3] / decimal.Decimal(100) + decimal.Decimal(5) * drivers[i][4][j][3])}', '{drivers[i][4][j][0]}')")
        write_conn.commit()

    write_cursor.close()
    write_conn.close()
    read_cursor.close()
    read_conn.close()
    return "OK"
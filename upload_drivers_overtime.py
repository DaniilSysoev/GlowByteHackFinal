import psycopg2
import datetime
import base64


def update_drivers_overtime():
    write_conn = psycopg2.connect(dbname='dwh', user='dwh_krasnoyarsk', 
                            password=base64.b64decode('ZHdoX2tyYXNub3lhcnNrX3VCUGFYTlN4').decode('utf-8'), host='de-edu-db.chronosavant.ru', sslmode='require')
    write_cursor = write_conn.cursor()


    #получаем максимальное start_dt - время когда мы последний раз обновляли данные и берем все что изменилось после него
    write_cursor.execute(f"SELECT MAX(start24h_dt) FROM rep_drivers_overtime;")
    max_update_dt = write_cursor.fetchall()
    if max_update_dt == [(None,)]:
        max_update_dt = datetime.datetime(2004, 9, 29)
    else:
        max_update_dt = max_update_dt[0][0]

    #Выбор новых записей
    write_cursor.execute(f"""SELECT waybill_num, driver_pers_num, work_start_dt, work_end_dt
                            FROM fact_waybills WHERE work_start_dt > '{max_update_dt}'
                            ORDER BY driver_pers_num, work_start_dt;""")
    res = write_cursor.fetchall()
    # Записываем в таблицу
    for now_waybill in res:
        #Проверка нарушения
        work_time = now_waybill[3]-now_waybill[2]
        if work_time > datetime.timedelta(hours=7):
            write_cursor.execute('INSERT INTO rep_drivers_overtime VALUES(%s, %s, %s);',
                                            (now_waybill[1], now_waybill[2], work_time))
            write_conn.commit()
        else:
            waybills_in_future = [n for n in res if now_waybill[1] == n[1] and now_waybill[0] != n[0] and n[2] > now_waybill[3]]
            if waybills_in_future != []:
                for waybill in waybills_in_future:
                    if waybill[2] < now_waybill[2] + datetime.timedelta(days=1):
                        if waybill[3] < now_waybill[2] + datetime.timedelta(days=1):
                            work_time += waybill[3] - waybill[2]
                        else:
                            work_time += now_waybill[2] + datetime.timedelta(days=1) - waybill[2]
                            break
                    break
                if work_time > datetime.timedelta(hours=7):
                    write_cursor.execute('INSERT INTO rep_drivers_overtime VALUES(%s, %s, %s);',
                                            (now_waybill[1], now_waybill[2], work_time))
                    write_conn.commit()
    
    write_cursor.close()
    write_conn.close()
    return 'OK'
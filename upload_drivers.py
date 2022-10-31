import psycopg2
import datetime
import base64


def update_dim_drivers():
    read_conn = psycopg2.connect(dbname='taxi', user='etl_tech_user', 
                            password=base64.b64decode('ZXRsX3RlY2hfdXNlcl9wYXNzd29yZA==').decode('utf-8'), host='de-edu-db.chronosavant.ru', sslmode='require')
    read_cursor = read_conn.cursor()

    write_conn = psycopg2.connect(dbname='dwh', user='dwh_krasnoyarsk', 
                            password=base64.b64decode('ZHdoX2tyYXNub3lhcnNrX3VCUGFYTlN4').decode('utf-8'), host='de-edu-db.chronosavant.ru', sslmode='require')
    write_cursor = write_conn.cursor()


    #получаем максимальное start_dt - время когда мы последний раз обновляли данные и берем все что изменилось после него
    write_cursor.execute(f"SELECT MAX(start_dt) FROM dim_drivers;")
    max_update_dt = write_cursor.fetchall()
    if max_update_dt == [(None,)]:
        max_update_dt = datetime.datetime(2004, 9, 29)
    else:
        max_update_dt = max_update_dt[0][0] + datetime.timedelta(hours=3)
    read_cursor.execute(f"SELECT * FROM main.drivers WHERE update_dt > '{max_update_dt}'")
    drivers = read_cursor.fetchall()


    #получение маскимального id записи  
    write_cursor.execute("SELECT MAX(personnel_num) FROM dim_drivers")
    res = write_cursor.fetchall()
    if res != [(None,)]:
        personnel_num = res[-1][0] + 1
    else:
        personnel_num = 0

    
    #запись новых данных
    for driver in drivers:              
        last_name = driver[2]
        first_name = driver[1]
        middle_name = driver[3]
        birth_dt = driver[7]
        card_num = driver[5]
        driver_license_num = driver[0]
        driver_license_dt = driver[4]
        delited_flag = 'N'
        end_dt = datetime.datetime(9999, 12, 31)
        
        
        # Проверка на поворение строки и обновление end_dt если есть повторы
        write_cursor.execute(f"SELECT * FROM dim_drivers WHERE driver_license_num = '{driver_license_num}';")
        carq = write_cursor.fetchall()
        if len(carq) > 0:
            last_line = carq[-1]
            if last_line[2:] == (last_name, first_name, middle_name, birth_dt, card_num,\
                                driver_license_num, driver_license_dt, delited_flag, end_dt):
                continue
            else:
                # print((personnel_num, last_name, first_name, middle_name, birth_dt,\
                #         card_num, driver_license_num, driver_license_dt, delited_flag, end_dt))
                write_cursor.execute(f"UPDATE dim_drivers SET end_dt = CURRENT_TIMESTAMP-interval '1 second' WHERE driver_license_num = '{driver_license_num}' AND end_dt = '{datetime.datetime(9999, 12, 31)}';")
        
        write_cursor.execute("INSERT INTO dim_drivers VALUES(%s, CURRENT_TIMESTAMP, %s, %s, %s, %s, %s, %s, %s, %s, %s);",
                    (personnel_num, last_name, first_name, middle_name, birth_dt,\
                        card_num, driver_license_num, driver_license_dt, delited_flag, end_dt))
        # print((personnel_num, last_name, first_name, middle_name, birth_dt,\
        #                 card_num, driver_license_num, driver_license_dt, delited_flag, end_dt))
        personnel_num += 1
        write_conn.commit()
        
        
    write_cursor.close()
    write_conn.close()
    read_cursor.close()
    read_conn.close()
    
    return 'OK'
    
    
if __name__ == '__main__':
    update_dim_drivers()
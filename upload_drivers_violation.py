import psycopg2
import base64


def update_drivers_violations():
    write_conn = psycopg2.connect(dbname='dwh', user='dwh_krasnoyarsk', 
                            password=base64.b64decode('ZHdoX2tyYXNub3lhcnNrX3VCUGFYTlN4').decode('utf-8'), host='de-edu-db.chronosavant.ru', sslmode='require')
    write_cursor = write_conn.cursor()


    #получаем максимальное ride_id - последнее ride_id которое мы точно проверяли 
    write_cursor.execute(f"SELECT MAX(ride) FROM rep_drivers_violations;")
    max_ride_id = write_cursor.fetchall()
    if max_ride_id == [(None,)]:
        max_ride_id = 0
    else:
        max_ride_id = max_ride_id[0][0]
    
    #Запись данных в таблицу
    write_cursor.execute(f"""SELECT driver_pers_num, ride_id, distance_val, ride_start_dt, ride_end_dt
                            FROM fact_rides WHERE ride_start_dt IS NOT NULL AND ride_id > {max_ride_id}
                            ORDER BY ride_start_dt, driver_pers_num;""") 
    rides = write_cursor.fetchall()
    for ride in rides:
        #Изменяем id поездки и среднюю скорость, но оствляем дату и персональный номер водителя
        personnel_num = ride[0]
        ride_id = ride[1]
        ride_time = (ride[4]-ride[3]).seconds # время поездки в секундах
        speed = round(ride[2]/ride_time*3600, 1)
        report_dt = ride[3]
        
        if speed > 85:
            write_cursor.execute(f"SELECT MAX(violations_cnt) FROM rep_drivers_violations WHERE personnel_num = {personnel_num};")
            violations_cnt = write_cursor.fetchall()
            if violations_cnt == [(None,)]:
                violations_cnt = 0
            else:
                violations_cnt = violations_cnt[0][0] + 1

            write_cursor.execute('INSERT INTO rep_drivers_violations VALUES(%s, %s, %s, %s, %s);',
                                            (personnel_num, ride_id, speed, violations_cnt, report_dt))
            write_conn.commit()
            
    
    write_cursor.close()
    write_conn.close()
    return 'OK'

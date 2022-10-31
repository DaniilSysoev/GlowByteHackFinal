import psycopg2
import datetime
import base64


def update_fact_rides():
    read_conn = psycopg2.connect(dbname='taxi', user='etl_tech_user', 
                            password=base64.b64decode('ZXRsX3RlY2hfdXNlcl9wYXNzd29yZA==').decode('utf-8'), host='de-edu-db.chronosavant.ru', sslmode='require')
    read_cursor = read_conn.cursor()


    write_conn = psycopg2.connect(dbname='dwh', user='dwh_krasnoyarsk', 
                            password=base64.b64decode('ZHdoX2tyYXNub3lhcnNrX3VCUGFYTlN4').decode('utf-8'), host='de-edu-db.chronosavant.ru', sslmode='require')
    write_cursor = write_conn.cursor()

    
    #получаем максимальное start_dt - время когда мы последний раз обновляли данные и берем все что изменилось после него
    write_cursor.execute(f"SELECT MAX(ride_end_dt) FROM fact_rides;")
    max_update_dt = write_cursor.fetchall()
    if max_update_dt == [(None,)]:
        max_update_dt = datetime.datetime(2004, 9, 29)
    else:
        max_update_dt = max_update_dt[0][0]


    read_cursor.execute(f"""SELECT point_from, point_to, distance, price, client_phone, ry.car_plate_num as car_plate_num,\
                                ry.dt as arrival_dt, NULL as start_dt, cl.dt as end_dt, cl.movement_id, ry.ride as ride_id
                        FROM (SELECT * FROM main.movement WHERE event = 'READY') AS ry
                            INNER JOIN (SELECT * FROM main.movement WHERE event = 'CANCEL') AS cl ON ry.ride = cl.ride
                            INNER JOIN main.rides as rides ON ry.ride = rides.ride_id
                            WHERE cl.dt > '{max_update_dt}';""", )
    rides = read_cursor.fetchall()
    
        
    read_cursor.execute(f"""SELECT point_from, point_to, distance, price, client_phone, ry.car_plate_num as car_plate_num,\
                                ry.dt as arrival_dt, bg.dt as start_dt, ed.dt as end_dt, ed.movement_id, ry.ride as ride_id
                        FROM (SELECT * FROM main.movement WHERE event = 'READY') AS ry
                            INNER JOIN (SELECT * FROM main.movement WHERE event = 'BEGIN') AS bg ON ry.ride = bg.ride
                            INNER JOIN (SELECT * FROM main.movement WHERE event = 'END') as ed ON bg.ride = ed.ride
                            INNER JOIN main.rides as rides ON ry.ride = rides.ride_id
                            WHERE ed.dt > '{max_update_dt}';""")
    rides += read_cursor.fetchall()
    
    for ride in rides:
        # print(ride)
        ride_id = ride[10]
        point_from_txt = ride[0]
        point_to_txt = ride[1]
        distance_val = ride[2]
        price_amt = ride[3]
        client_phone_num = ride[4]
        car_plate_num = ride[5]
        ride_arrival_dt = ride[6]
        ride_start_dt = ride[7]
        ride_end_dt = ride[8]
        
        write_cursor.execute(f"SELECT driver_pers_num FROM fact_waybills WHERE car_plate_num = '{car_plate_num}' AND work_start_dt <= '{ride_arrival_dt}' AND work_end_dt >= '{ride_end_dt}';")
        res = write_cursor.fetchall()
        if res == []:
            driver_pers_num = -1
        else:
            driver_pers_num = int(res[-1][0])
            
        # print((ride_id, point_from_txt, point_to_txt, distance_val, price_amt, client_phone_num,\
        #     driver_pers_num, car_plate_num, ride_arrival_dt, ride_start_dt, ride_end_dt))
        
        write_cursor.execute('INSERT INTO fact_rides VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);',
                    (ride_id, point_from_txt, point_to_txt, distance_val, price_amt, client_phone_num,\
                        driver_pers_num, car_plate_num, ride_arrival_dt, ride_start_dt, ride_end_dt))
        write_conn.commit()
        

    write_cursor.close()
    write_conn.close()
    read_cursor.close()
    read_conn.close()
    
    return 'OK'
    

if __name__ == '__main__':
    update_fact_rides()
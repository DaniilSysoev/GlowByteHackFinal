import psycopg2
import datetime
import base64


def update_dim_cars():
    read_conn = psycopg2.connect(dbname='taxi', user='etl_tech_user', 
                            password=base64.b64decode('ZXRsX3RlY2hfdXNlcl9wYXNzd29yZA==').decode('utf-8'), host='de-edu-db.chronosavant.ru', sslmode='require')
    read_cursor = read_conn.cursor()


    write_conn = psycopg2.connect(dbname='dwh', user='dwh_krasnoyarsk', 
                            password=base64.b64decode('ZHdoX2tyYXNub3lhcnNrX3VCUGFYTlN4').decode('utf-8'), host='de-edu-db.chronosavant.ru', sslmode='require')
    write_cursor = write_conn.cursor()

    
    #получаем максимальное start_dt - время когда мы последний раз обновляли данные и берем все что изменилось после него
    write_cursor.execute(f"SELECT MAX(start_dt) FROM dim_cars ;")
    max_update_dt = write_cursor.fetchall()
    if max_update_dt == [(None,)]:
        max_update_dt = datetime.datetime(2004, 9, 29)
    else:
        max_update_dt = max_update_dt[0][0] + datetime.timedelta(hours=3)
    
    read_cursor.execute(f"SELECT * FROM main.car_pool WHERE update_dt > '{max_update_dt}'")
    cars = read_cursor.fetchall()
    
    for car in cars:        
        plate_num = car[0]
        model_name = car[1]
        revision_dt = car[2]
        deleted_flag = car[4]
        end_dt = datetime.datetime(9999, 12, 31)
        
        # Проверка на поворение строки и обновление end_dt если есть повторы
        write_cursor.execute(f"SELECT * FROM dim_cars WHERE plate_num = '{plate_num}';")
        update_car = write_cursor.fetchall()
        if len(update_car) > 0:
            last_line = update_car[-1]
            
            if (tuple([last_line[0]])+last_line[2:]) == (plate_num, model_name, revision_dt, deleted_flag, end_dt):
                continue
            else:
                write_cursor.execute(f"UPDATE dim_cars SET end_dt = CURRENT_TIMESTAMP-interval '1 second' WHERE plate_num = '{plate_num}' AND end_dt = '{datetime.datetime(9999, 12, 31)}';")
        
        write_cursor.execute('INSERT INTO dim_cars VALUES(%s, CURRENT_TIMESTAMP, %s, %s, %s, %s);',
                    (plate_num, model_name, revision_dt, deleted_flag, end_dt))
        # print((plate_num, model_name, revision_dt, deleted_flag, end_dt))
        write_conn.commit()


    write_cursor.close()
    write_conn.close()
    read_cursor.close()
    read_conn.close()
    
    return 'OK'
    

if __name__ == '__main__':
    update_dim_cars()
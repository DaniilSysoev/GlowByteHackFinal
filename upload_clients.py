import psycopg2
import datetime
import base64


def update_dim_clients():
    read_conn = psycopg2.connect(dbname='taxi', user='etl_tech_user', 
                            password=base64.b64decode('ZXRsX3RlY2hfdXNlcl9wYXNzd29yZA==').decode('utf-8'), host='de-edu-db.chronosavant.ru', sslmode='require')
    read_cursor = read_conn.cursor()


    write_conn = psycopg2.connect(dbname='dwh', user='dwh_krasnoyarsk', 
                            password=base64.b64decode('ZHdoX2tyYXNub3lhcnNrX3VCUGFYTlN4').decode('utf-8'), host='de-edu-db.chronosavant.ru', sslmode='require')
    write_cursor = write_conn.cursor()


    #получаем максимальное start_dt - время когда мы последний раз обновляли данные и берем все что изменилось после него
    write_cursor.execute(f"SELECT MAX(start_dt) FROM dim_clients;")
    max_update_dt = write_cursor.fetchall()
    if max_update_dt == [(None,)]:
        max_update_dt = datetime.datetime(2004, 9, 29)
    else:
        max_update_dt = max_update_dt[0][0] + datetime.timedelta(hours=3)
    read_cursor.execute(f"SELECT * FROM main.rides WHERE dt > '{max_update_dt}' ORDER BY ride_id")
    clients = read_cursor.fetchall()
    

    for client in clients:
        start_dt = client[1]
        phone_num = client[2]
        card_num = client[3]
        deleted_flag = 'N'
        end_dt = datetime.datetime(9999, 12, 31)
        
        # Проверка на поворение строки и обновление end_dt если есть повторы
        write_cursor.execute(f"SELECT * FROM dim_clients WHERE phone_num = '{phone_num}' ORDER BY start_dt;")
        update_client = write_cursor.fetchall()
        if len(update_client) > 0:
            last_line = update_client[-1]
            if (last_line[0], last_line[2], last_line[3], last_line[4]) == (phone_num, card_num, deleted_flag, end_dt):
                continue
            else:
                write_cursor.execute(f"UPDATE dim_clients SET end_dt = '{start_dt - datetime.timedelta(seconds=1)}' WHERE phone_num = '{phone_num}' AND end_dt = '{datetime.datetime(9999, 12, 31)}';")
        
        # print((start_dt, phone_num, card_num, deleted_flag, end_dt))
        write_cursor.execute('INSERT INTO dim_clients VALUES(%s, %s, %s, %s, %s);',
                    (phone_num, start_dt, card_num, deleted_flag, end_dt))
        write_conn.commit()
        
            
    write_cursor.close()
    write_conn.close()
    read_cursor.close()
    read_conn.close()
    
    return 'OK'
    

if __name__ == '__main__':
    update_dim_clients()
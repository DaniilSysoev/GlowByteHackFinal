import psycopg2
import datetime
import os
import csv
from ftp_dowload import dowload_new_payments
import base64


def update_fact_payments():
    write_conn = psycopg2.connect(dbname='dwh', user='dwh_krasnoyarsk', 
                        password=base64.b64decode('ZHdoX2tyYXNub3lhcnNrX3VCUGFYTlN4').decode('utf-8'), host='de-edu-db.chronosavant.ru', sslmode='require')
    write_cursor = write_conn.cursor()

    # payment_2022-10-28_20-30.csv
    write_cursor.execute('SELECT MAX(transaction_dt) FROM fact_payments;')
    max_payment_dt = write_cursor.fetchall()
    if max_payment_dt == [(None,)]:
        max_payment_dt = 'payment_2022-10-12_15-00.csv'
    else:
        max_payment_dt = max_payment_dt[0][0]
        
        minute = '30'
        hour = max_payment_dt.hour
        if max_payment_dt.minute > 30:
            minute = '00'
            hour += 1
        max_payment_dt = f'payment_{max_payment_dt.year}-{max_payment_dt.month}-{max_payment_dt.day}_{hour}-{minute}.csv'
    
        
    #Загрузка новых файлов
    dowload_new_payments(max_payment_dt)


    directory = 'payments/'
    files = os.listdir(directory)

    wrong_lines = []

    write_cursor.execute("SELECT MAX(transaction_id) FROM fact_payments")
    res = write_cursor.fetchall()
    if res != [(None,)]:
        transaction_id = res[-1][0] + 1
    else:
        transaction_id = 0
        
    # print(files)
    for file in files:
        with open('payments/'+file, newline='') as f:
            spamreader = csv.reader(f)
            for row in spamreader:
                try:
                    #выборка данных
                    data = row[0].split('\t')
                    card_num = int(data[1])
                    transaction_amt = float(data[2])
                    transaction_dt = datetime.datetime.strptime(data[0], '%d.%m.%Y %H:%M:%S')
                    # print((transaction_id, card_num, transaction_amt, transaction_dt))

                    
                    #запись данных
                    write_cursor.execute('INSERT INTO fact_payments VALUES(%s, %s, %s, %s);',
                        (transaction_id, card_num, transaction_amt, transaction_dt))
                    write_conn.commit()
                    
                    transaction_id += 1
                except Exception as e:
                    wrong_lines.append((transaction_id, card_num, transaction_amt, transaction_dt))
                    
        os.remove('payments/'+file)


    write_cursor.close()
    write_conn.close()
    
    if wrong_lines != []:
        print('Problems with files:' + '\n'.join(wrong_lines))
        
    return wrong_lines
    
    
if __name__ == '__main__':
    update_fact_payments()
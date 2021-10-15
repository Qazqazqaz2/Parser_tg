import pandas as pd
import psycopg2
import openpyxl

def data_in_href(datas):
    new_datas = []
    for data in datas:
        print(data)
        data_href = 'https://t.me/'+str(data[1])
        new_datas.append([data[0], data_href, data[2], data[3]])
    print(new_datas)

    return new_datas

def conn_database():
    try:
        conn = psycopg2.connect(dbname=f"accounts", user="postgres", password="762341Aa", host="localhost", port="6432")
        cur = conn.cursor()
        cur.execute("SELECT name, username, followers, lang FROM accounts_list_2;")
        data = cur.fetchall()
        cur.close()
        if conn is not None:
            conn.close()
    except Exception as e:
        print('\n\n\n'+str(e)+'\n\n\n')
    finally:
        return data_in_href(data)

def main(data):
    print(data)
    df = pd.DataFrame(data[0:60000], columns=['a', 'v', 'w', 'l'])
    df.to_excel('./main_140_0_60000.xlsx', sheet_name="main_1")


if __name__ == '__main__':
    main(conn_database())

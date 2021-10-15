from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from fake_useragent import UserAgent
from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver
import time
import argparse
from selenium.webdriver.firefox.firefox_binary import FirefoxBinary
from tbselenium.tbdriver import TorBrowserDriver
from tbselenium.utils import launch_tbb_tor_with_stem
import tbselenium.common as cm
import psycopg2
import requests
from bs4 import BeautifulSoup
from psycopg2.pool import ThreadedConnectionPool
from multiprocessing import Process
from datetime import datetime, timedelta
import threading
from concurrent.futures import ThreadPoolExecutor, wait, ProcessPoolExecutor
from multiprocessing import Queue
from threading import Semaphore




INSERT_QUERY = "INSERT INTO accounts_list_2 (name, username, followers, lang) VALUES "



proxies = {'http':'socks5://127.0.0.1:9050',
         'https': 'socks5://127.0.0.1:9050'
         }



def save_in_postgres(name, username, followers, lang):
    try:
        conn = psycopg2.connect(dbname=f"accounts", user="postgres", password="762341Aa", host="localhost", port="6432")
        cur = conn.cursor()
        cur.execute(INSERT_QUERY + f"('{name}', '{username}', {followers}, {lang});")
        conn.commit()
        cur.close()
        if conn is not None:
            conn.close()
    except Exception as e:
        print('\n\n\n'+str(e)+'\n\n\n')

def get_all_values():
    conn = psycopg2.connect(dbname=f"accounts", user="postgres", password="762341Aa", host="localhost", port="6432")
    cur = conn.cursor()
    cur.execute("SELECT * FROM accounts_list_2;")
    try:
        all_values_name = cur.fetchall()
    except Exception as e:
        print(e)
    cur.close()
    if conn is not None:
        conn.close()
    return all_values_name



class ReallyThreadedConnectionPool(ThreadedConnectionPool):
    def __init__(self, minconn, maxconn, *args, **kwargs):
        self._semaphore = Semaphore(maxconn)
        super().__init__(minconn, maxconn, *args, **kwargs)

    def getconn(self, *args, **kwargs):
        self._semaphore.acquire()
        return super().getconn(*args, **kwargs)


# writer write data to queue
class PsqlMultiThreadExample(object):
    _select_conn_count = 10;
    _insert_conn_count = 10;
    _insert_conn_pool = None;
    _select_conn_pool = None;

    def __init__(self):
        self = self;

    def postgres_connection(self):
        """ Connect to the PostgreSQL database server """
        conn = None
        try:
            # read connection parameters

            # connect to the PostgreSQL server
            conn = psycopg2.connect(dbname=f"accounts", user="postgres", password="762341Aa", host="localhost", port="6432")

            # create a cursor
            cur = conn.cursor()

            # execute a statement
            cur.execute('SELECT version()')

            # display the PostgreSQL database server version
            db_version = cur.fetchone()
            print(db_version)

            # close the communication with the PostgreSQL
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()

    def check_connection(self):
        """ Checking the postgres database connection"""
        conn = None;
        try:
            conn = PsqlMultiThreadExample._select_conn_pool.getconn()

            # create a cursor
            cur = conn.cursor()

            # execute a statement
            cur.execute('SELECT version()')

            # display the PostgreSQL database server version
            db_version = cur.fetchone()
            print(db_version)
            # close the communication with the PostgreSQL
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()


    def create_connection_pool(self):
        """ Create the thread safe threaded postgres connection pool"""

        # calculate the max and min connection required
        max_conn = PsqlMultiThreadExample._insert_conn_count + PsqlMultiThreadExample._select_conn_count;
        min_conn = max_conn / 2;

        # creating separate connection for read and write purpose
        PsqlMultiThreadExample._insert_conn_pool = PsqlMultiThreadExample._select_conn_pool \
            = ReallyThreadedConnectionPool(min_conn, max_conn, dbname=f"accounts", user="postgres", password="762341Aa", host="localhost", port="6432");

    def read_data(self, values):
        """
        This read thedata from the postgres and shared those records with each
        processor to perform their operation using threads
        Here we calculate the pardition value to help threading to read data from database

        :return:
        """
        pardition_value = 805000 / 10 # Its total record
        data = 0
        # this helps to identify the starting number to get data from db
        start_index = 1
        insert_conn = PsqlMultiThreadExample._insert_conn_pool.getconn()
        insert_conn.autocommit = 1
        ps = Process(target=self.process_data, args=(data, insert_conn))
        ps.daemon = True
        ps.start()
        ps.join()

    def process_data(self, data, insert_conn):
        """
        Here we process the each process into 10 multiple threads to do data process

        :param queue: 
        :param pid:
        :param start_index:
        :param end_index:
        :param select_conn:
        :param insert_conn:
        :return:
        """
        urs = []
        langs = ['ru', 'fr', 'en', 'uz', 'fa', 'es', 'it', 'de', 'pt', 'tr']
        with ThreadPoolExecutor(10) as executor:
            for page in range(1760, 5000):
                ins_cur = insert_conn.cursor()
                executor.submit(self.process_thread, ins_cur, threading.Lock(), page)
        
    def process_thread(self, ins_cur, lock, page):
        print(page)
        lang = 'na'
        try:
            resp = requests.get(f'https://en.tgchannels.org/category/misc?page={str(page)}&size=30&lang=all', proxies=proxies)
            soup = BeautifulSoup(resp.content, "html.parser")
        except Exception as e:
            print(e)
        all_items = soup.find_all('a', class_="channel-item")
        for item in all_items:
            username = str(item['href']).split('?start')[0]
            username = username.replace('/channel/', '')
            main_block = item.find('div', class_="channel-item__caption")
            name = main_block.find('h1').text
            if "'" in name:
                name = name.replace("'", "''")
            followers = main_block.find('span').text
            if followers=='':
                followers = '0'
            try:
                lock.acquire()
                ins_cur.execute(INSERT_QUERY + f"('{name}' ,'{username}', '{followers}', '{lang}');")
            except Exception as e:
                print(e)
            lock.release()
            


        
    def write_data(self, records, ins_cur, lock):

        lock.acquire()
        if records and records != '':
            insert_com = str(INSERT_QUERY) + str(records)
            ins_cur.execute(insert_com)
        lock.release()


if __name__ == '__main__':
    cmp_clener = PsqlMultiThreadExample();
    #Craeting database connection pool to help connection shared along process
    cmp_clener.create_connection_pool()
    cmp_clener.read_data(get_all_values())


from threading import Thread
import mysql.connector
import time

def connect(host='localhost', db='zd_shema_tup', user='david', passwd='1234'):
    return mysql.connector.connect(host=host, database=db, user=user, password=passwd)

def insert(n, commit, sleep):
    db = connect()
    cursor = db.cursor()
    for i in range(n):
        cursor.execute('INSERT INTO zdravstveni_domovi (ime_zd, naslov_zd) VALUES (%s, %s)', ['ZD '+str(i), 'Naslov '+str(i)])
    time.sleep(sleep)
    if commit == 1:
        db.commit()
    else:
        db.rollback()
    

def select(table='zdravstveni_domovi'):
    db = connect()
    cursor = db.cursor()
    cursor.execute('SELECT * FROM '+table)
    query_result = cursor.fetchall()
    for entry in query_result:
        print(entry)

def select_twice(table='zdravstveni_domovi'):
    db = connect()
    cursor = db.cursor()
    cursor.execute('SELECT * FROM '+table)
    query_result = cursor.fetchall()
    for entry in query_result:
        print(entry)
    time.sleep(4)
    cursor.execute('SELECT * FROM '+table)
    query_result = cursor.fetchall()
    for entry in query_result:
        print(entry)

def select_twice_where():
    db = connect()
    cursor = db.cursor()
    cursor.execute('SELECT * FROM zdravstveni_domovi WHERE ime_zd LIKE "ZD%"')
    query_result = cursor.fetchall()
    for entry in query_result:
        print(entry)
    time.sleep(4)
    cursor.execute('SELECT * FROM zdravstveni_domovi WHERE ime_zd LIKE "ZD%"')
    query_result = cursor.fetchall()
    for entry in query_result:
        print(entry)

def update(id, new_value, commit, sleep):
    db = connect()
    cursor = db.cursor()
    cursor.execute('SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE')
    cursor.execute('UPDATE zdravstveni_domovi SET ime_zd = %s WHERE id_zd = %s', [new_value, id])
    time.sleep(sleep)
    if commit == 1:
        db.commit()
    else:
        db.rollback()

def dirty_read():
    db = connect()
    cursor = db.cursor()
    cursor.execute('SET GLOBAL TRANSACTION ISOLATION LEVEL READ UNCOMMITTED')
    Thread(target = insert, args=(3, 0, 2.0)).start()
    Thread(target = select).start()
    time.sleep(5.0)
    Thread(target = select).start()
    time.sleep(2.0)
    cursor.execute('SET GLOBAL TRANSACTION ISOLATION LEVEL REPEATABLE READ')

def non_rep_read():
    db = connect()
    cursor = db.cursor()
    cursor.execute('SET GLOBAL TRANSACTION ISOLATION LEVEL READ COMMITTED')
    Thread(target = select_twice).start()
    Thread(target = update, args=(1, 'updated', 1, 0)).start()
    time.sleep(6.0)
    cursor.execute('SET GLOBAL TRANSACTION ISOLATION LEVEL REPEATABLE READ')

def first():
    db = connect()
    cursor = db.cursor()
    cursor.execute('UPDATE zdravstveni_domovi SET naslov_zd = "prvi" WHERE id_zd = 1')
    db.commit()

def second():
    db = connect()
    cursor = db.cursor()
    cursor.execute('UPDATE zdravstveni_domovi SET naslov_zd = "drugi" WHERE id_zd = 1')
    db.commit()


def phantom_read():
    db = connect()
    cursor = db.cursor()
    cursor.execute('SET GLOBAL TRANSACTION ISOLATION LEVEL READ COMMITTED')
    Thread(target = select_twice_where).start()
    Thread(target = update, args=(3, 'updated', 1, 0)).start()
    time.sleep(6.0)
    cursor.execute('SET GLOBAL TRANSACTION ISOLATION LEVEL REPEATABLE READ')


def lost_update():
    Thread(target = first).start()
    Thread(target = second).start()
    time.sleep(1)
    Thread(target = select).start()
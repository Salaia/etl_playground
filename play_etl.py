# This is a sample Python script.
import requests
import pandas
import mysql.connector
import pendulum
import csv
import os, tempfile
from datetime import datetime
import json

"""
    создай (если не создана) там таблицу "all emails"
    
    Подумай, в чем временно хранить данные... их же мало извлечь, их надо преобразовать
    Есть ли у пандас временная табличка какая-то? И как это потом отправить в БД...
    
    Посмотри, как записывает мужик с Airflow.
"""

sql_init = """
            CREATE TABLE IF NOT EXISTS emails_all_sources (
            user_name varchar(1000)
            , email varchar(1000)
            , source varchar(1000)
            , pipe_time timestamp
            );
"""
sql_insert_all_emails = """
            INSERT INTO emails_all_sources(user_name, email, source, pipe_time)
            VALUES(%s, %s, %s, %s)
"""

def run_pipeline():

    # Все будет писаться в базу учебника "Знакомьтесь, Питон!" - а чем она плоха?
    try:  # mysql.connector.connect
        db_connect = mysql.connector.connect(
            user='puma', password='My_Puma_Is_Cool_5', database='hello_python_mysql_db')
    except Exception as err:
        print(err)
    try:
        sql_cursor = db_connect.cursor()
        sql_cursor.execute(sql_init)
        sql_cursor.close()

    except Exception as err:
        print(err)
    #finally:
        #db_connect.close()

    # ********  URL ******************
    # В качестве жертвы у меня librarianmon
    api_url = "http://127.0.0.1:8182/users"  # Example API endpoint

    # Make the GET request
    response = requests.get(api_url)

    # Check for successful response (status code 200)
    if response.status_code == 200:
        # Parse the JSON response into a Python dictionary or list
        data = response.json()
        try:
            sql_cursor = db_connect.cursor()
            for row in data:
                row_tuple = (f"{row['name']}", f"{row['email']}", 'url(librarian)', datetime.now())
                sql_cursor.execute(sql_insert_all_emails, row_tuple)
            db_connect.commit()
        except Exception as err :
            print(err)
        finally:
            sql_cursor.close()
    else:
        print(f"Error: Unable to retrieve data. Status code: {response.status_code}")
        print(response.text)  # Print the error message if available






# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    run_pipeline()

    #test = """TEST"""
    #print(test)
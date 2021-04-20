# Consider key uniqueness
# psycopg - postgresql db adapter

import random
import string
import time
import datetime
import json
from psycopg2 import connect, Error
import sys

def generate_random_json():
        letters = string.ascii_uppercase
        json_list = []

        for y in range(0, 20):
            key = ""
            for i in range(0, 4):
                # CHAR OR INT ?
                heads_or_tails = random.randint(0, 1)
                if (heads_or_tails == 0):
                    elem = random.randint(0, 9)
                    elem = str(elem)
                else:
                    elem = random.choice(letters)
                key = key + elem

            value = round(random.uniform(0, 100), 2)
            ct = datetime.datetime.now()
            json_list.append(
            {
                "key": key,
                "value": value,
                "ts": str(ct)
            })

            json_object = json.dumps(json_list[y])
            insert_into_db(json_object)
            time.sleep(0.250)


def read_file():
    json_file = open(r'json-files\data_file_generated.txt', "r")
    json_list = json_file.readlines()
    y = len(json_list)

    for y in range(len(json_list)):
        print(json_list[y])
        insert_into_db(json_list[y])
        time.sleep(0.250)


def insert_into_db(json_object):

        json_dict = json.loads(json_object)

        try:
            conn_obj = connect(
                dbname="etl_db",
                user="postgres",
                host="127.0.0.1",
                password="postgres",
                connect_timeout=3
            )
            cur = conn_obj.cursor()

        except (Exception, Error) as err:
            print("\npsycopg2 connect error:", err)
            conn = None
            cur = None

        # SQL statement:
        columns = list(json_dict.keys())
        print("\ncolumn names:", columns)
        values_str = (json_dict['key'], json_dict['value'], json_dict['ts'])

        shema_name = "dbo."
        table_name = "json_raw"

        sql_string = "INSERT INTO %s (%s)\nVALUES %s" % \
                     (
                         shema_name + table_name,
                         ', '.join(columns),
                         values_str
                     )

        print("\nSQL statement:")
        print(sql_string)

        if cur != None:

            try:
                cur.execute(sql_string)
                conn_obj.commit()

                print('\nfinished INSERT INTO execution')

            except (Exception, Error) as error:
                print("\nexecute_sql() error:", error)
                conn_obj.rollback()

            cur.close()
            conn_obj.close()

def main():
    #read_file()
    generate_random_json()

main()



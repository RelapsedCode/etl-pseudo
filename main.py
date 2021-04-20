import random
import string
import time
import datetime
import json
import sys
from psycopg2 import connect, Error


class ETL:

    def __generate_random_json(self):
        letters = string.ascii_uppercase
        json_list = []
        json_object_list = []
        # for y in range(0, 10):
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
        json_object = json.dumps(json_list[0])

        return json_object
        # time.sleep(0.250)

    def __read_from_file(self, path_to_file):
        json_file = open(path_to_file, "r")
        json_list = json_file.readlines()
        return json_list

    def __insert_into_db(self, json_object):
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

        columns = list(json_dict.keys())
        # print("\ncolumn names:", columns)
        values_str = (json_dict['key'], json_dict['value'], json_dict['ts'])

        shema_name = "dbo."
        table_name = "json_raw"

        sql_string = "INSERT INTO %s (%s)\nVALUES %s" % \
                     (
                         shema_name + table_name,
                         ', '.join(columns),
                         values_str
                     )

        # print("\nSQL statement:")
        # print(sql_string)

        if cur != None:

            try:
                cur.execute(sql_string)
                conn_obj.commit()
                # print('\nfinished INSERT INTO execution')

            except (Exception, Error) as error:
                print("\nexecute_sql() error:", error)
                conn_obj.rollback()

            cur.close()
            conn_obj.close()

    def source(self, source):
        self._source = source
        return self

    def sink(self, sink):
        self._sink = sink
        return self

    def run(self):
        if self._source == "Simulation":
            while True:
                    temp_json = self.__generate_random_json()
                    if self._sink == "PostgreSQL":
                        self.__insert_into_db(temp_json)
                        pass
                    elif self._sink == "Console":
                        print(temp_json, "\n")

        else:
            temp_list = self.__read_from_file(self._source)
            if self._sink == "PostgreSQL":
                for z in range(len(temp_list)):
                    self.__insert_into_db(temp_list[z])
                pass
            elif self._sink == "Console":
                print(f"\n".join(temp_list))


ETL().source("Simulation").sink("Console").run()

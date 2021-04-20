import random
import string
import datetime
import json

def generate_random_json():
    key = ""
    value = 0
    data = {}
    letters = string.ascii_uppercase
    x = 0

    with open(r"json-files\data_file2.txt", "w") as write_file:
        write_file.write("")

    for x in range(1, 100):
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

        json_dict = {
            "key": key,
            "value": value,
            "ts": str(ct)
        }

        json_object = json.dumps(json_dict)
        print(json_dict, end="\n" + json_object)

        #Generate file containing json data
        with open(r"json-files\data_file2.txt", "a") as write_file:
            write_file.write(json_object + "\n")

generate_random_json()
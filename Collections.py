import json


def check_id_presence():
    data, ids = [], set()
    with open("Resources/data_keywords", encoding="utf-8") as fl:
        for line in fl.readlines():
            data.append(line)
            js = json.loads(line)
            if js["id"] not in ids:
                ids.add(js["id"])
    if len(data) == len(ids):
        print("All data have unique ids")
    else:
        print("All data doesn't have unique ids")

if __name__ == '__main__':
    check_id_presence()
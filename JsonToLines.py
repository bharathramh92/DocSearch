import json

def main():
    with open("data_keywords.json", encoding="utf-8") as fl:
        data = json.loads(fl.read())
    with open("data_keywords", mode= 'a', encoding="utf-8") as fl:
        for dt in data:
            fl.write(json.dumps(dt) + "\n")

if __name__ == '__main__':
    main()
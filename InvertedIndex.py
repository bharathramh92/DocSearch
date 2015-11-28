#using spark

import sys
import os
from pyspark import SparkContext
import json
import re


if len(sys.argv) != 2:
    print("Usage: linreg <datafile>")
    exit(-1)
os.environ['SPARK_HOME']="/home/bharath/spark-1.5.1"
os.environ['PYSPARK_PYTHON'] = "/usr/bin/python3"
os.environ['PYSPARK_DRIVER_PYTHON'] = "ipython3"
sc = SparkContext(appName="DataStructureDef")


def main():
    # Input yx file has y_i as the first element of each line
    # and the remaining elements constitute x_i
    # attribute_weight = {'publisher', 'authors', 'description', 'id', 'ISBN_10', 'maturityRating', 'title', 'infoLink', 'ISBN_13', 'pageCount'}
    # dict_keys(['imageLinks', 'title', 'ISBN_13', 'maturityRating', 'ISBN_10', 'id', 'pageCount', 'description', 'authors', 'infoLink', 'publisher'])
    # Total books 128326
    # Validated that all data have unique id. Therefore using id as the identifier for books
    data = sc.textFile("Resources/data_keywords")

    def author_zone_map_helper(line):
        entity = "authors"
        book = json.loads(line)
        authors_combinations_dict = {}
        if entity in book:
            for author in book[entity]:
                for auth_split in re.findall(r"[a-zA-Z]+", author):
                    authors_combinations_dict[auth_split.lower()] = [book["id"]]
        return tuple(authors_combinations_dict.items())

    def title_zone_map_helper(line):
        entity = "title"
        book = json.loads(line)
        title_combinations_dict = {}
        if entity in book:
            author = book[entity]
            for title_split in re.findall(r"[a-zA-Z]+", author):
                title_combinations_dict[title_split.lower()] = [book["id"]]
        return tuple(title_combinations_dict.items())

    def publisher_zone_map_helper(line):
        entity = "publisher"
        book = json.loads(line)
        publisher_combinations_dict = {}
        if entity in book:
            author = book[entity]
            for auth_split in re.findall(r"[a-zA-Z]+", author):
                publisher_combinations_dict[auth_split.lower()] = [book["id"]]
        return tuple(publisher_combinations_dict.items())

    def keyWords_zone_map_helper(line):
        entity = "keyWords"
        book = json.loads(line)
        keyWord_combinations_dict = {}
        if entity in book:
            keyWords = book[entity]
            for keyWord in keyWords:
                keyWord_combinations_dict[keyWord.lower()] = [book["id"]]
        return tuple(keyWord_combinations_dict.items())

    def isbn_zone_map_helper(line):
        entities = ["ISBN_10", "ISBN_13"]
        isbn_combinations_dict = {}
        for entity in entities:
            book = json.loads(line)
            if entity in book:
                isbn = book[entity]
                isbn_combinations_dict[isbn.lower()] = [book["id"]]
        return tuple(isbn_combinations_dict.items())

    author_zone = data.flatMap(author_zone_map_helper).reduceByKey(lambda x, y: x + y)
    title_zone = data.flatMap(title_zone_map_helper).reduceByKey(lambda x, y: x + y)
    publisher_zone = data.flatMap(publisher_zone_map_helper).reduceByKey(lambda x, y: x + y)
    keyWords_zone = data.flatMap(keyWords_zone_map_helper).reduceByKey(lambda x, y: x + y)
    isbn_zone = data.flatMap(isbn_zone_map_helper).reduceByKey(lambda x, y: x + y)

    print(author_zone.count())
    print(title_zone.count())
    print(publisher_zone.count())
    print(keyWords_zone.count())
    print(isbn_zone.count())

    sc.stop()
if __name__ == '__main__':
    main()
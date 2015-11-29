#using spark
import sys
import os
from pyspark import SparkContext
import json
import re
from nltk.stem.wordnet import WordNetLemmatizer


os.environ['SPARK_HOME']="/home/bharath/spark-1.5.1"
os.environ['PYSPARK_PYTHON'] = "/usr/bin/python3"
os.environ['PYSPARK_DRIVER_PYTHON'] = "ipython3"
sc = SparkContext(appName="DataStructureDef")


def main():
    # Total books 128326
    # Validated that all data have unique id. Therefore using id as the identifier for books.
    # Ref. data_validation.py
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
        lemmatizer = WordNetLemmatizer()
        book = json.loads(line)
        title_combinations_dict = {}
        if entity in book:
            author = book[entity]
            for title_split in re.findall(r"[a-zA-Z]+", author):
                title_combinations_dict[lemmatizer.lemmatize(title_split.lower())] = [book["id"]]
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
        lemmatizer = WordNetLemmatizer()
        book = json.loads(line)
        keyWord_combinations_dict = {}
        if entity in book:
            keyWords = book[entity]
            for keyWord in keyWords:
                keyWord_combinations_dict[lemmatizer.lemmatize(keyWord.lower())] = [book["id"]]
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

    def category_zone_map_helper(line):
        entity = "categories"
        lemmatizer = WordNetLemmatizer()
        book = json.loads(line)
        category_combinations_dict = {}
        if entity in book:
            for category in book[entity]:
                for cat_split in re.findall(r"[a-zA-Z]+", category):
                    category_combinations_dict[lemmatizer.lemmatize(cat_split.lower())] = [book["id"]]
        return tuple(category_combinations_dict.items())

    author_zone = data.flatMap(author_zone_map_helper).reduceByKey(lambda x, y: x + y)
    title_zone = data.flatMap(title_zone_map_helper).reduceByKey(lambda x, y: x + y)
    publisher_zone = data.flatMap(publisher_zone_map_helper).reduceByKey(lambda x, y: x + y)
    key_words_zone = data.flatMap(keyWords_zone_map_helper).reduceByKey(lambda x, y: x + y)
    isbn_zone = data.flatMap(isbn_zone_map_helper).reduceByKey(lambda x, y: x + y)
    category_zone = data.flatMap(category_zone_map_helper).reduceByKey(lambda x, y: x + y)
    author_zone.saveAsTextFile("Resources/author_rdd")
    title_zone.saveAsTextFile("Resources/title_rdd")
    publisher_zone.saveAsTextFile("Resources/publisher_rdd")
    key_words_zone.saveAsTextFile("Resources/keyWords_rdd")
    isbn_zone.saveAsTextFile("Resources/isbn_rdd")
    category_zone.saveAsTextFile("Resources/category_rdd")
    sc.stop()

if __name__ == '__main__':
    main()
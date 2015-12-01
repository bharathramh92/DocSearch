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
sc = SparkContext(appName="InvertedIndex")


def main():
    # Total books 128326
    # Validated that all data have unique id. Therefore using id as the identifier for books.
    # Ref. Collections.py
    # lemma for title_zone, category_zone, keyword_zone
    data = sc.textFile("Resources/id_doc_rdd_raw")

    def author_zone_map_helper(line):
        entity = "authors"
        book = json.loads(line)
        authors_combinations_dict = {}
        for book_id, book_data in book.items():
            if entity in book_data:
                for author in book_data[entity]:
                    for auth_split in re.findall(r"[a-zA-Z]+", author):
                        authors_combinations_dict[auth_split.lower()] = [book_id]
        return tuple(authors_combinations_dict.items())

    def title_zone_map_helper(line):
        entity = "title"
        lemmatizer = WordNetLemmatizer()
        book = json.loads(line)
        title_combinations_dict = {}
        for book_id, book_data in book.items():
            if entity in book_data:
                author = book_data[entity]
                for title_split in re.findall(r"[a-zA-Z0-9]+", author):
                    title_combinations_dict[lemmatizer.lemmatize(title_split.lower())] = [book_id]
        return tuple(title_combinations_dict.items())

    def publisher_zone_map_helper(line):
        entity = "publisher"
        book = json.loads(line)
        publisher_combinations_dict = {}
        for book_id, book_data in book.items():
            if entity in book_data:
                author = book_data[entity]
                for auth_split in re.findall(r"[a-zA-Z0-9]+", author):
                    publisher_combinations_dict[auth_split.lower()] = [book_id]
        return tuple(publisher_combinations_dict.items())

    def keyWords_zone_map_helper(line):
        entity = "keyWords"
        lemmatizer = WordNetLemmatizer()
        book = json.loads(line)
        keyWord_combinations_dict = {}
        for book_id, book_data in book.items():
            if entity in book_data:
                keyWords = book_data[entity]
                for keyWord in keyWords:
                    keyWord_combinations_dict[lemmatizer.lemmatize(keyWord.lower())] = [book_id]
        return tuple(keyWord_combinations_dict.items())

    def isbn_zone_map_helper(line):
        entities = ["ISBN_10", "ISBN_13"]
        isbn_combinations_dict = {}
        for entity in entities:
            book = json.loads(line)
            for book_id, book_data in book.items():
                if entity in book_data:
                    isbn = book_data[entity]
                    isbn = "".join(isbn.split("-"))         # Stripping off - in ISBN-13
                    isbn_combinations_dict[isbn.lower()] = [book_id]
        return tuple(isbn_combinations_dict.items())

    def category_zone_map_helper(line):
        entity = "categories"
        lemmatizer = WordNetLemmatizer()
        book = json.loads(line)
        category_combinations_dict = {}
        for book_id, book_data in book.items():
            if entity in book_data:
                for category in book_data[entity]:
                    for cat_split in re.findall(r"[a-zA-Z0-9]+", category):
                        category_combinations_dict[lemmatizer.lemmatize(cat_split.lower())] = [book_id]
        return tuple(category_combinations_dict.items())


    def index_map_helper(line):
        entities = ["ISBN_10", "ISBN_13", "categories", "authors", "title", "publisher", "keyWords"]
        lemma_entities = ["title", "categories", "keyWords"]
        lemmatizer = WordNetLemmatizer()
        book = json.loads(line)
        index = []
        for entity in entities:
            for book_id, book_data in book.items():
                if entity in book_data:
                    value_for_entity = book_data[entity]
                    if isinstance(value_for_entity, str):
                        value_for_entity = [value_for_entity]
                    for term in value_for_entity:
                        for term_split in re.findall(r"[a-zA-Z0-9\-]+", term):
                            term_split = term_split.lower()
                            if entity in lemma_entities:
                                term_split = lemmatizer.lemmatize(term_split)
                            index.append((term_split, ((book_id, entity), )))
        return index


    # author_zone = data.flatMap(author_zone_map_helper).reduceByKey(lambda x, y: x + y)
    # title_zone = data.flatMap(title_zone_map_helper).reduceByKey(lambda x, y: x + y)
    # publisher_zone = data.flatMap(publisher_zone_map_helper).reduceByKey(lambda x, y: x + y)
    # key_words_zone = data.flatMap(keyWords_zone_map_helper).reduceByKey(lambda x, y: x + y)
    # isbn_zone = data.flatMap(isbn_zone_map_helper).reduceByKey(lambda x, y: x + y)
    # category_zone = data.flatMap(category_zone_map_helper).reduceByKey(lambda x, y: x + y)
    # author_zone.saveAsTextFile("Resources/author_rdd")
    # title_zone.saveAsTextFile("Resources/title_rdd")
    # publisher_zone.saveAsTextFile("Resources/publisher_rdd")
    # key_words_zone.saveAsTextFile("Resources/keyWords_rdd")
    # isbn_zone.saveAsTextFile("Resources/isbn_rdd")
    # category_zone.saveAsTextFile("Resources/category_rdd")

    index_rdd = data.flatMap(index_map_helper).reduceByKey(lambda x, y: x + y)
    index_rdd.saveAsTextFile("Resources/index_rdd")

    sc.stop()

if __name__ == '__main__':
    main()
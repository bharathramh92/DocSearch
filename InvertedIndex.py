#using spark
import os
from pyspark import SparkContext
import json
import re
from nltk.stem.wordnet import WordNetLemmatizer
from IndexConstants import ENTITIES, LEMMA_ENTITIES

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

    def index_map_helper(line):
        lemmatizer = WordNetLemmatizer()
        book = json.loads(line)     # loads each book which were in a row of id_doc_rdd_raw
        index = []
        for entity in ENTITIES:
            for book_id, book_data in book.items():     # book data key is doc id and value is the book data
                if entity in book_data:                 # value is a dictionary with entity/zone mapping to its value
                    value_for_entity = book_data[entity]
                    if isinstance(value_for_entity, str):   # if the entity value is string, convert it to list
                        value_for_entity = [value_for_entity]   # eg: "isbn":"123" --> "isbn" = ["123",]
                    for term in value_for_entity:               # each entity value is taken
                        for term_split in re.findall(r"[a-zA-Z0-9\-]+", term):  # multiple words are split and indexed
                            term_split = term_split.lower()     # eg: thomas h cormen --> into ['thomas', 'h', 'cormen']
                            if entity in LEMMA_ENTITIES:
                                term_split = lemmatizer.lemmatize(term_split)   # lemmatizing if they have to be
                            index.append((term_split, ((book_id, entity), )))
        return index

    # flatmap is used since multiple key, value pairs are to be send out in map phase
    index_rdd = data.flatMap(index_map_helper).reduceByKey(lambda x, y: x + y)
    # index_rdd.saveAsTextFile("Resources/index_rdd")
    print(index_rdd.count())
    sc.stop()

if __name__ == '__main__':
    main()
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

    index_rdd = data.flatMap(index_map_helper).reduceByKey(lambda x, y: x + y)
    index_rdd.saveAsTextFile("Resources/index_rdd")

    sc.stop()

if __name__ == '__main__':
    main()
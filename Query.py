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
    # Query
    # a_docs = author_zone.filter(lambda line: "cormen" in line).collect()
    # k_docs = keyWords_zone.filter(lambda line: "algorithm" in line).collect()
    # print(k_docs)
    author_zone = sc.textFile("Resources/author_rdd")
    title_zone = sc.textFile("Resources/title_rdd")
    publisher_zone = sc.textFile("Resources/publisher_rdd")
    keyWords_zone = sc.textFile("Resources/keyWords_rdd")
    isbn_zone = sc.textFile("Resources/isbn_rdd")
    category_zone = sc.textFile("Resources/category_rdd")

    query_term = "encyclopedia - vin-/.i"
    query_term_list, lemmatizer = [], WordNetLemmatizer()
    for term in re.findall(r"[a-zA-Z]+", query_term):
        query_term_list.append(lemmatizer.lemmatize(term.lower()))
    print(query_term_list)
    sc.stop()

if __name__ == '__main__':
    main()
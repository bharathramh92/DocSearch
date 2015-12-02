import os
from pyspark import SparkContext
import json


def write_id_doc_rdd(sc):
    """
    Method to make id_doc rdd from data_keywords doc.
    data_keywords should have book record in each line(json format)
    output generated file is rdd with mapping of id against the corresponding book data
    :param sc: spark context
    :return: None
    """
    def id_doc_mapper(data):
        data = json.loads(data)
        id = data.pop('id')
        return json.dumps({id: data})
    data_in = sc.textFile("Resources/data_keywords")
    id_doc_rdd = data_in.map(id_doc_mapper)
    id_doc_rdd.saveAsTextFile("Resources/id_doc_rdd_raw")
    sc.stop()


def read_docs(id_list, sc):
    """
    Get doc data from id_doc_rdd_raw
    :param id_list: list of ids of the book
    :return: dictionary of books mapped using id
    """
    data_in = sc.textFile("Resources/id_doc_rdd_raw")

    def filter_helper(line):
        line = json.loads(line)
        return list(line.keys())[0] in id_list
    doc_map = {}
    for dt in data_in.filter(filter_helper).collect():
        for k, v in (json.loads(dt)).items():
            doc_map[k] = v
    sc.stop()
    return doc_map


def call_write_data():
    os.environ['SPARK_HOME'] = "/home/bharath/spark-1.5.1"
    os.environ['PYSPARK_PYTHON'] = "/usr/bin/python3"
    os.environ['PYSPARK_DRIVER_PYTHON'] = "ipython3"
    sc = SparkContext(appName="SparkCollections")
    write_id_doc_rdd(sc)

if __name__ == '__main__':
    # call_write_data()
    # print(read_docs(['asAAMBGmZR0C']))
    pass
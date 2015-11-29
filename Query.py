#using spark
import sys
import os
from pyspark import SparkContext
import json
import re
from nltk.stem.wordnet import WordNetLemmatizer
from collections import defaultdict

os.environ['SPARK_HOME'] = "/home/bharath/spark-1.5.1"
os.environ['PYSPARK_PYTHON'] = "/usr/bin/python3"
os.environ['PYSPARK_DRIVER_PYTHON'] = "ipython3"
sc = SparkContext(appName="DataStructureDef")

model_weightage = {'isbn_zone': .3, 'author_zone': .18, 'publisher_zone': .15, 'title_zone': .15, 'category_zone': .12,
                   'key_word_zone': .1}

def get_docs():
    author_zone = sc.textFile("Resources/author_rdd")
    title_zone = sc.textFile("Resources/title_rdd")
    publisher_zone = sc.textFile("Resources/publisher_rdd")
    key_words_zone = sc.textFile("Resources/keyWords_rdd")
    isbn_zone = sc.textFile("Resources/isbn_rdd")
    category_zone = sc.textFile("Resources/category_rdd")

    # To get restricted search results
    # Usage: If the search has to be restricted only to author and title, remaining 4 booleans should be False
    strict_not_auth, strict_not_title, strict_not_publisher = True, True, True
    strict_not_key_words, strict_not_isbn, strict_not_category = True, True, True

    # query_term = 'cormen thomas data algorithm'
    # query_term = "5324302432 abc-5324302432"
    query_term =  "9780262033848 9780262259460"
    query_term = "cormen thomas"
    query_term_docs, lemmatizer = {}, WordNetLemmatizer()

    for term in re.findall(r"([\w]+[\-]*[\w]+)", query_term):
        term_documents = {}
        term_lemma = lemmatizer.lemmatize(term.lower())   # Lemmatized term
        # Lemmatized term and term will have numbers. Hence no problem in taking any of those for number presence check.
        # Checking whether a number is present or not
        num_split = re.findall("[0-9]+", term)
        has_number = len(num_split) >= 1

        # following search term will be None if those zones doesn't have to searched for.
        auth_term, title_term, publisher_term = None, None, None
        key_words_term, isbn_term, category_term = None, None, None

        if strict_not_auth and not has_number:
            auth_term = term            # author term will never have a number

        if strict_not_title:
            title_term = term_lemma     # lemmatized and can have numbers as well

        if strict_not_publisher:
            publisher_term = term       # publisher can't have numbers

        if strict_not_key_words:
            key_words_term = term_lemma     # lemmatized and can have numbers

        if strict_not_category:
            category_term = term_lemma    # lemmatized and can have numbers

        if strict_not_isbn and has_number:
            num_isbn_split = "".join(term.split("-"))
            if num_isbn_split.isdigit() and (len(num_isbn_split) == 10 or len(num_isbn_split) == 13):
                isbn_term = term        # should be a number and length >= 10
                # if isbn is the term, no need to check any other zones.
                auth_term = title_term = publisher_term = key_words_term = category_term = None

        print("\nRaw Term %s" % term)
        print("auth_term %s, title_term %s, publisher_term %s, key_words_term %s, isbn_term %s, category_term %s"
              % (auth_term, title_term, publisher_term, key_words_term, isbn_term, category_term))

        auth_docs, title_docs, publisher_docs = [], [], []
        key_words_docs, isbn_docs, category_docs = [], [], []
        if auth_term is not None:
            try:
                raw_docs_collections = author_zone.filter(lambda line: term == eval(line)[0])
                docs_collect = raw_docs_collections.collect()
                auth_docs = eval(docs_collect[0])[1]
                # print("author ", auth_docs)
                term_documents['author_zone'] = tuple(auth_docs)
            except:
                pass

        if title_term is not None:
            try:
                raw_docs_collections = title_zone.filter(lambda line: term == eval(line)[0])
                docs_collect = raw_docs_collections.collect()
                title_docs = eval(docs_collect[0])[1]
                # print("title ", title_docs)
                term_documents['title_zone'] = tuple(title_docs)
            except:
                pass

        if publisher_term is not None:
            try:
                raw_docs_collections = publisher_zone.filter(lambda line: term == eval(line)[0])
                docs_collect = raw_docs_collections.collect()
                publisher_docs = eval(docs_collect[0])[1]
                # print("publisher ", publisher_docs)
                term_documents['publisher_zone'] = tuple(publisher_docs)
            except:
                pass

        if key_words_term is not None:
            try:
                raw_docs_collections = key_words_zone.filter(lambda line: term == eval(line)[0])
                docs_collect = raw_docs_collections.collect()
                key_words_docs = eval(docs_collect[0])[1]
                # print("key_words ", key_words_docs)
                term_documents['key_word_zone'] = tuple(key_words_docs)
            except:
                pass

        if isbn_term is not None:
            try:
                raw_docs_collections = isbn_zone.filter(lambda line: term == eval(line)[0])
                docs_collect = raw_docs_collections.collect()
                isbn_docs = eval(docs_collect[0])[1]
                # print("isbn ", isbn_docs)
                term_documents['isbn_zone'] = tuple(isbn_docs)
            except:
                pass

        if category_term is not None:
            try:
                raw_docs_collections = category_zone.filter(lambda line: term == eval(line)[0])
                docs_collect = raw_docs_collections.collect()
                category_docs = eval(docs_collect[0])[1]
                # print("category ", category_docs)
                term_documents['isbn_zone'] = tuple(isbn_docs)
                term_documents.update(category_docs)
            except:
                pass

        # print("Term documents for %s\n %s" % (term, term_documents))
        query_term_docs[term] = term_documents
    return query_term_docs


def main():
    # Query
    # a_docs = author_zone.filter(lambda line: "cormen" in line).collect()
    # k_docs = keyWords_zone.filter(lambda line: "algorithm" in line).collect()
    # print(k_docs)

    docs_dict = get_docs()
    weighted_docs_dict = defaultdict(int)
    # terms_sorted = sorted(docs_dict, key=lambda doc_list_key: len(docs_dict[doc_list_key]))
    # for k in terms_sorted:
    #     print(k, " ", len(docs_dict[k]))
    # previous_computed_doc_ids = terms_sorted[0]
    # for term in terms_sorted[1:]:
    #     next_doc_ids = set()
    #     if len(docs_dict[previous_computed_doc_ids]) <= len(docs_dict[term]):
    #         short = docs_dict[previous_computed_doc_ids]
    #         long  = docs_dict[term]
    #     else:
    #         long = docs_dict[previous_computed_doc_ids]
    #         short = docs_dict[term]
    #     for item_in_short in short:
    #         if item_in_short in long:
    #             next_doc_ids.add(item_in_short)
    #     previous_computed_doc_ids = next_doc_ids
    # print(len(previous_computed_doc_ids))
    # print(previous_computed_doc_ids)
    # print(docs_dict)
    for k, v in docs_dict.items():
        for entity, doc_ids in v.items():
            for doc_id in doc_ids:
                weighted_docs_dict[doc_id] += model_weightage[entity]*1
    ranking_key = sorted(weighted_docs_dict, key= lambda key: weighted_docs_dict[key])
    ranking_score_list = []
    for doc_id in ranking_key:
        ranking_score_list.append((doc_id, weighted_docs_dict[doc_id]))
    print(ranking_score_list)
    sc.stop()

if __name__ == '__main__':
    main()
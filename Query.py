#using spark
import sys
import os
from pyspark import SparkContext
import json
import re
from nltk.stem.wordnet import WordNetLemmatizer
from collections import defaultdict
from SparkCollection import read_docs
os.environ['SPARK_HOME'] = "/home/bharath/spark-1.5.1"
os.environ['PYSPARK_PYTHON'] = "/usr/bin/python3"
os.environ['PYSPARK_DRIVER_PYTHON'] = "ipython3"
sc = SparkContext(appName="Query")

model_weightage = {'ISBN_10': .3, 'ISBN_13': .3, 'authors': .18, 'publisher': .15, 'title': .15, 'categories': .12,
                   'keyWords': .1}


def get_docs(query_term):
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

    # query_term = 'data algorithm'
    # query_term = "5324302432 abc-5324302432"
    # query_term =  "9780262033848 9780262259460"
    # query_term = "cormen thomas"
    query_term_docs, lemmatizer = {}, WordNetLemmatizer()
    term_ids_mapping = {}

    for term in re.findall(r"([\w]+[\-]*[\w]+)", query_term):
        term_documents = defaultdict(list)
        ids = set()
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
                for doc_id in auth_docs:
                    term_documents[doc_id].append("author_zone")
                ids.update(auth_docs)
            except:
                pass

        if title_term is not None:
            try:
                raw_docs_collections = title_zone.filter(lambda line: term == eval(line)[0])
                docs_collect = raw_docs_collections.collect()
                title_docs = eval(docs_collect[0])[1]
                # print("title ", title_docs)
                for doc_id in title_docs:
                    term_documents[doc_id].append("title_zone")
                ids.update(title_docs)
            except:
                pass

        if publisher_term is not None:
            try:
                raw_docs_collections = publisher_zone.filter(lambda line: term == eval(line)[0])
                docs_collect = raw_docs_collections.collect()
                publisher_docs = eval(docs_collect[0])[1]
                # print("publisher ", publisher_docs)
                for doc_id in publisher_docs:
                    term_documents[doc_id].append("publisher_zone")
                ids.update(publisher_docs)
            except:
                pass

        if key_words_term is not None:
            try:
                raw_docs_collections = key_words_zone.filter(lambda line: term == eval(line)[0])
                docs_collect = raw_docs_collections.collect()
                key_words_docs = eval(docs_collect[0])[1]
                # print("key_words ", key_words_docs)
                for doc_id in key_words_docs:
                    term_documents[doc_id].append("key_word_zone")
                ids.update(key_words_docs)
            except:
                pass

        if isbn_term is not None:
            try:
                raw_docs_collections = isbn_zone.filter(lambda line: term == eval(line)[0])
                docs_collect = raw_docs_collections.collect()
                isbn_docs = eval(docs_collect[0])[1]
                # print("isbn ", isbn_docs)
                term_documents['isbn_zone'] = tuple(isbn_docs)
                for doc_id in isbn_docs:
                    term_documents[doc_id].append("isbn_zone")
                ids.update(isbn_docs)
            except:
                pass

        if category_term is not None:
            try:
                raw_docs_collections = category_zone.filter(lambda line: term == eval(line)[0])
                docs_collect = raw_docs_collections.collect()
                category_docs = eval(docs_collect[0])[1]
                # print("category ", category_docs)
                for doc_id in category_docs:
                    term_documents[doc_id].append("category_zone")
                ids.update(category_docs)
            except:
                pass

        # print("Term documents for %s\n %s" % (term, term_documents))
        query_term_docs[term] = term_documents
        term_ids_mapping[term] = ids

    terms_names_sorted = sorted(term_ids_mapping, key=lambda k: len(term_ids_mapping[k]))
    for k in terms_names_sorted:
        print(k, " ", len(term_ids_mapping[k]))
    previous_computed_doc_ids = terms_names_sorted[0]
    anded_result = set()
    if len(terms_names_sorted) > 1:
        for term in terms_names_sorted[1:]:
            if len(term_ids_mapping[previous_computed_doc_ids]) <= len(term_ids_mapping[term]):
                short = term_ids_mapping[previous_computed_doc_ids]
                long = term_ids_mapping[term]
            else:
                long = term_ids_mapping[previous_computed_doc_ids]
                short = term_ids_mapping[term]
            short = sorted(short)
            long = sorted(long)
            for item_in_short in short:
                if item_in_short in long:
                    anded_result.add(item_in_short)
            previous_computed_doc_ids = anded_result
    else:
        anded_result.update(term_ids_mapping[previous_computed_doc_ids])

    return query_term_docs, anded_result


def get_docs_zone(query_term, zone_restriction):
    zone_rdd = sc.textFile("Resources/index_rdd")
    # To get restricted search results
    # Usage: If the search has to be restricted only to author and title, remaining 4 booleans should be False
    strict_not_auth, strict_not_title, strict_not_publisher = True, True, True
    strict_not_key_words, strict_not_isbn, strict_not_category = True, True, True

    query_term_docs, lemmatizer = {}, WordNetLemmatizer()
    term_ids_mapping = {}
    term_combos = set()
    for term in re.findall(r"([\w]+[\-]*[\w]+)", query_term):
        ##add lemma possibilities,with or without hiphen combos
        if len(term.split("-")) > 1:
            # hiphen present
            for tm in re.findall(r"[a-zA-Z]+[\-]+[\w]+", term):
                term_combos.add(tm)
            if len(re.findall(r"[0-9]+[\-]+[\w]+", term)) > 0:
                term_combos.add(''.join(term.split("-")))
        else:
            term_combos.add(term)
    for term in term_combos:
        lemma_term = lemmatizer.lemmatize(term)
        term_combos.add(lemma_term)


    term_documents = defaultdict(list)
    lemma_entities = ["title", "categories", "keyWords"]
    # ids = set()
    # term_lemma = lemmatizer.lemmatize(term.lower())   # Lemmatized term
    # Lemmatized term and term will have numbers. Hence no problem in taking any of those for number presence check.
    # Checking whether a number is present or not
    # num_split = re.findall("[0-9]+", term)
    # has_number = len(num_split) >= 1

    # following search term will be None if those zones doesn't have to searched for.
    # auth_term, title_term, publisher_term = None, None, None
    # key_words_term, isbn_term, category_term = None, None, None

        # if strict_not_auth and not has_number:
        #     auth_term = term            # author term will never have a number
        #
        # if strict_not_title:
        #     title_term = term_lemma     # lemmatized and can have numbers as well
        #
        # if strict_not_publisher:
        #     publisher_term = term       # publisher can't have numbers
        #
        # if strict_not_key_words:
        #     key_words_term = term_lemma     # lemmatized and can have numbers
        #
        # if strict_not_category:
        #     category_term = term_lemma    # lemmatized and can have numbers

    def raw_map_helper(line):
        line = eval(line)
        return line[0] in term_combos

    # raw_docs_collections = zone_rdd.filter(lambda line: eval(line)[0] in term_combos)
    raw_docs_collections = zone_rdd.filter(raw_map_helper)
    docs_collect = raw_docs_collections.collect()
    if len(docs_collect) > 0:
        for term_doc_zone in docs_collect:
            term_doc_zone = eval(term_doc_zone)
            term = term_doc_zone[0]
            doc_zone = term_doc_zone[1]
            ids = set()
            for doc, zone in doc_zone:
                print(doc, ", ", zone)
                if zone in zone_restriction:
                    ids.add(doc)
                    term_documents[doc].append(zone)
            term_ids_mapping[term] = ids
            query_term_docs[term] = term_documents
                # # print("author ", auth_docs)
                # for doc_id in auth_docs:
                #     term_documents[doc_id].append("author_zone")
                # ids.update(auth_docs)





    #     if strict_not_isbn and has_number:
    #         num_isbn_split = "".join(term.split("-"))
    #         if num_isbn_split.isdigit() and (len(num_isbn_split) == 10 or len(num_isbn_split) == 13):
    #             isbn_term = term        # should be a number and length >= 10
    #             # if isbn is the term, no need to check any other zones.
    #             auth_term = title_term = publisher_term = key_words_term = category_term = None
    #
    #     print("\nRaw Term %s" % term)
    #     print("auth_term %s, title_term %s, publisher_term %s, key_words_term %s, isbn_term %s, category_term %s"
    #           % (auth_term, title_term, publisher_term, key_words_term, isbn_term, category_term))
    #
    #     auth_docs, title_docs, publisher_docs = [], [], []
    #     key_words_docs, isbn_docs, category_docs = [], [], []
    #     if auth_term is not None:
    #         try:
    #             raw_docs_collections = author_zone.filter(lambda line: term == eval(line)[0])
    #             docs_collect = raw_docs_collections.collect()
    #             auth_docs = eval(docs_collect[0])[1]
    #             # print("author ", auth_docs)
    #             for doc_id in auth_docs:
    #                 term_documents[doc_id].append("author_zone")
    #             ids.update(auth_docs)
    #         except:
    #             pass
    #
    #     if title_term is not None:
    #         try:
    #             raw_docs_collections = title_zone.filter(lambda line: term == eval(line)[0])
    #             docs_collect = raw_docs_collections.collect()
    #             title_docs = eval(docs_collect[0])[1]
    #             # print("title ", title_docs)
    #             for doc_id in title_docs:
    #                 term_documents[doc_id].append("title_zone")
    #             ids.update(title_docs)
    #         except:
    #             pass
    #
    #     if publisher_term is not None:
    #         try:
    #             raw_docs_collections = publisher_zone.filter(lambda line: term == eval(line)[0])
    #             docs_collect = raw_docs_collections.collect()
    #             publisher_docs = eval(docs_collect[0])[1]
    #             # print("publisher ", publisher_docs)
    #             for doc_id in publisher_docs:
    #                 term_documents[doc_id].append("publisher_zone")
    #             ids.update(publisher_docs)
    #         except:
    #             pass
    #
    #     if key_words_term is not None:
    #         try:
    #             raw_docs_collections = key_words_zone.filter(lambda line: term == eval(line)[0])
    #             docs_collect = raw_docs_collections.collect()
    #             key_words_docs = eval(docs_collect[0])[1]
    #             # print("key_words ", key_words_docs)
    #             for doc_id in key_words_docs:
    #                 term_documents[doc_id].append("key_word_zone")
    #             ids.update(key_words_docs)
    #         except:
    #             pass
    #
    #     if isbn_term is not None:
    #         try:
    #             raw_docs_collections = isbn_zone.filter(lambda line: term == eval(line)[0])
    #             docs_collect = raw_docs_collections.collect()
    #             isbn_docs = eval(docs_collect[0])[1]
    #             # print("isbn ", isbn_docs)
    #             term_documents['isbn_zone'] = tuple(isbn_docs)
    #             for doc_id in isbn_docs:
    #                 term_documents[doc_id].append("isbn_zone")
    #             ids.update(isbn_docs)
    #         except:
    #             pass
    #
    #     if category_term is not None:
    #         try:
    #             raw_docs_collections = category_zone.filter(lambda line: term == eval(line)[0])
    #             docs_collect = raw_docs_collections.collect()
    #             category_docs = eval(docs_collect[0])[1]
    #             # print("category ", category_docs)
    #             for doc_id in category_docs:
    #                 term_documents[doc_id].append("category_zone")
    #             ids.update(category_docs)
    #         except:
    #             pass
    #
    #     # print("Term documents for %s\n %s" % (term, term_documents))
    #     query_term_docs[term] = term_documents
    #     term_ids_mapping[term] = ids
    #
    terms_names_sorted = sorted(term_ids_mapping, key=lambda k: len(term_ids_mapping[k]))
    for k in terms_names_sorted:
        print(k, " ", len(term_ids_mapping[k]))
    previous_computed_doc_ids = terms_names_sorted[0]
    anded_result = set()
    if len(terms_names_sorted) > 1:
        for term in terms_names_sorted[1:]:
            if len(term_ids_mapping[previous_computed_doc_ids]) <= len(term_ids_mapping[term]):
                short = term_ids_mapping[previous_computed_doc_ids]
                long = term_ids_mapping[term]
            else:
                long = term_ids_mapping[previous_computed_doc_ids]
                short = term_ids_mapping[term]
            short = sorted(short)
            long = sorted(long)
            for item_in_short in short:
                if item_in_short in long:
                    anded_result.add(item_in_short)
            previous_computed_doc_ids = anded_result
    else:
        anded_result.update(term_ids_mapping[previous_computed_doc_ids])

    return query_term_docs, anded_result




def main():
    query_term_docs, anded_result = get_docs('algorithm cormen 309428042938')
    weighted_docs_dict = defaultdict(int)
    doc_rank_data = defaultdict(list)
    for term, doc_zone in query_term_docs.items():
        # for doc_id, zones in doc_zone.items():
        for doc_id in anded_result:
            for zone in doc_zone[doc_id]:
                weighted_docs_dict[doc_id] += model_weightage[zone]*1
            doc_rank_data[doc_id].append({term: doc_zone[doc_id]})
    ranking_key = sorted(weighted_docs_dict, key=lambda key: weighted_docs_dict[key], reverse=True)
    print("ids", ranking_key)
    ranked_score_list = []
    for doc_id in ranking_key:
        ranked_score_list.append((doc_id, weighted_docs_dict[doc_id]))

    print(ranked_score_list)
    print(doc_rank_data)
    print(read_docs(ranking_key, sc))
    sc.stop()


def new_main():
    # q_term = 'algorithm cormen'
    q_term = "9781478427674"
    q_term = "978-1478427674"
    zone_restriction = ["ISBN_10"]
    query_term_docs, anded_result = get_docs_zone(q_term, zone_restriction)
    weighted_docs_dict = defaultdict(int)
    doc_rank_data = defaultdict(list)
    for term, doc_zone in query_term_docs.items():
        for doc_id in anded_result:
            for zone in doc_zone[doc_id]:
                weighted_docs_dict[doc_id] += model_weightage[zone]*1
            doc_rank_data[doc_id].append({term: doc_zone[doc_id]})
    ranking_key = sorted(weighted_docs_dict, key=lambda key: weighted_docs_dict[key], reverse=True)
    print("ids", ranking_key)
    ranked_score_list = []
    for doc_id in ranking_key:
        ranked_score_list.append((doc_id, weighted_docs_dict[doc_id]))

    print(ranked_score_list)
    print(doc_rank_data)
    # print(read_docs(ranking_key, sc))
    sc.stop()

if __name__ == '__main__':
    # main()
    new_main()
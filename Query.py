#using spark
import os
from pyspark import SparkContext
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
zone_list = ['publisher', 'authors', 'categories', 'keyWords', 'ISBN_13', 'title', 'ISBN_10']


def get_docs(query_term, zone_restriction=None):
    """

    :param query_term:
    :param zone_restriction: list of zone name where the query term has to be matched with
    :return:
    """
    zone_rdd = sc.textFile("Resources/index_rdd")
    query_term_docs, lemmatizer = {}, WordNetLemmatizer()
    term_ids_mapping = {}
    term_combos = set()
    if zone_restriction is None:
        zone_restriction = {}
        for zn in zone_list:
            zone_restriction[zn] = query_term
    zone_restriction_added_terms = defaultdict(list)
    for zone_res, q_term in zone_restriction.items():
        for term in re.findall(r"([\w]+[\-]*[\w]+)", q_term):
            # Getting each term from the given search term
            if len(term.split("-")) > 1:
                if len(re.findall(r"[a-zA-Z]+[\-]+[\w]+", term)) > 0:
                    # if the term is a character and has a hiphen, split the term at the hiphen position
                    for tm in term.split("-"):
                        term_combos.add(tm)
                        zone_restriction_added_terms[zone_res].append(tm)
                if len(re.findall(r"[0-9]+[\-]+[\w]+", term)) > 0:
                    # if the term is a number and has a hiphen, concatenate the term
                    # useful in isbn search were the isbn could be given like ‎978-3-16-148410-0
                    combined_term = ''.join(term.split("-"))
                    term_combos.add(combined_term)
                    zone_restriction_added_terms[zone_res].append(combined_term)
            else:
                # if hiphen is not present in a term, add directly
                term_combos.add(term)
                zone_restriction_added_terms[zone_res].append(term)
    for term in term_combos:
        # add lemma possibilities as well in the search group
        lemma_term = lemmatizer.lemmatize(term)
        term_combos.add(lemma_term)

    term_documents = defaultdict(list)

    print(term_combos)
    def raw_map_helper(line):
        line = eval(line)
        return line[0] in term_combos

    # Performing data retrieval/filter using Spark
    # raw_docs_collections = zone_rdd.filter(lambda line: eval(line)[0] in term_combos)
    raw_docs_collections = zone_rdd.filter(raw_map_helper)
    docs_collect = raw_docs_collections.collect()   # list of documents from search query

    # Changing data format of the queried data
    if len(docs_collect) > 0:
        for term_doc_zone in docs_collect:
            term_doc_zone = eval(term_doc_zone)
            term = term_doc_zone[0]
            # data format: term_doc_zone: (index_term, tuple((document_id, zone_name), ))
            doc_zone = term_doc_zone[1]
            ids = set()     # to store the docs which matches to the zone condition
            for doc, zone in doc_zone:
                if term in zone_restriction_added_terms[zone]:
                    ids.add(doc)
                    term_documents[doc].append(zone)
            term_ids_mapping[term] = ids
            query_term_docs[term] = term_documents

    # And operation for all the term result
    terms_names_sorted = sorted(term_ids_mapping, key=lambda k: len(term_ids_mapping[k]))
    print("Records for each search terms")
    for k in terms_names_sorted:
        print(k, " ", len(term_ids_mapping[k]))
    anded_result = term_ids_mapping[terms_names_sorted[0]]
    # anded_result = set()
    if len(terms_names_sorted) > 1:
        for term in terms_names_sorted[1:]:
            # print(term, " , ", anded_result)
            if len(anded_result) <= len(term_ids_mapping[term]):
                short = anded_result
                long = term_ids_mapping[term]
            else:
                long = anded_result
                short = term_ids_mapping[term]
            short = sorted(short)
            long = sorted(long)
            for item_in_short in short:
                if item_in_short in long:
                    anded_result.add(item_in_short)
            # previous_computed_doc_ids = anded_result
    # else:
    #     anded_result.update(term_ids_mapping[previous_computed_doc_ids])

    return query_term_docs, anded_result


def main():
    q_term = 'algorithm'
    # q_term = "9781478427674"
    # q_term = "978-1478427674"

    # zone_restriction = ["ISBN_10"]
    zone_restriction = {'title': 'cormen algorithm', 'ISBN_10': '1478427671'}

    query_term_docs, anded_result = get_docs(q_term)
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
    print(read_docs(ranking_key, sc))
    sc.stop()

if __name__ == '__main__':
    main()
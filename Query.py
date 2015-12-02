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


def get_docs(query_term=None, zone_restriction=None):
    """

    :param query_term: search term if it has to be searched across all the zones.
    :param zone_restriction: dictionary whose keys are zone and the corresponding value is the search term of that zone.
    :return:
    """
    if query_term is None and zone_restriction is None:
        # Validation to make sure that search term is provided
        print("Either query_term of zone_restriction should passed.")
        exit(1)

    zone_rdd = sc.textFile("Resources/index_rdd")  # input rdd
    query_term_docs = {}   # in order to get weighted score, terms are mapped to a dictionary of doc id to zone mapping.
    lemmatizer = WordNetLemmatizer()
    term_ids_mapping = {}   # to map term with positive search result document ids
    term_combos = set()     # to store possible combinations of the search term
    if zone_restriction is None:
        # normalizing the search term to dictionary format of zone_restriction
        zone_restriction = {}
        for zn in zone_list:
            zone_restriction[zn] = query_term
    zone_restriction_added_terms = defaultdict(list)    # expand the zone_restriction to subset of the individual terms.
    for zone_res, q_term in zone_restriction.items():
        for term in re.findall(r"([\w]+[\-]*[\w]+)", q_term):
            # Getting individual words from the given search term
            if len(term.split("-")) > 1:
                if len(re.findall(r"[a-zA-Z]+[\-]+[\w]+", term)) > 0:
                    # if the term is a character and has a hiphen, split the term at the hiphen position
                    # 'abcd-efgh' will be transformed into ['abcd', 'efgh']
                    for tm in term.split("-"):
                        term_combos.add(tm)
                        zone_restriction_added_terms[zone_res].append(tm)
                if len(re.findall(r"[0-9]+[\-]+[\w]+", term)) > 0:
                    # if the term is a number and has a hiphen, concatenate the term. Mainly for isbn check
                    # eg: ‎978-3-16-148410-0 is transformed into 9783161484100
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

    print(term_combos)

    # Performing data retrieval/filter using Spark
    # raw_docs_collections = zone_rdd.filter(lambda line: eval(line)[0] in term_combos)
    def raw_map_helper(line):
        line = eval(line)
        # line[0] corresponds to index term
        return line[0] in term_combos
    raw_docs_collections = zone_rdd.filter(raw_map_helper)
    docs_collect = raw_docs_collections.collect()   # list of documents from search query

    # Changing data format of the queried data
    if len(docs_collect) > 0:
        for term_doc_zone in docs_collect:
            term_doc_zone = eval(term_doc_zone)
            term = term_doc_zone[0]
            term_documents = defaultdict(list)  # mapping doc id with zones
            # data format: term_doc_zone: (index_term, tuple((document_id, zone_name), ))
            doc_zone = term_doc_zone[1]
            ids = set()     # to store the docs for corresponding term
            for doc, zone in doc_zone:
                if term in zone_restriction_added_terms[zone]:
                    # save the result only if the data corresponds to the given zone
                    ids.add(doc)
                    term_documents[doc].append(zone)
            term_ids_mapping[term] = ids
            query_term_docs[term] = term_documents

    # And operations for the term results
    # sort the query terms based on the result size
    terms_names_sorted = sorted(term_ids_mapping, key=lambda k: len(term_ids_mapping[k]))
    print("Records for each search terms")
    for q_term_name in terms_names_sorted:
        print(q_term_name, " ", len(term_ids_mapping[q_term_name]))
    anded_result = term_ids_mapping[terms_names_sorted[0]]  # storing the small sized result to anded_result
    if len(terms_names_sorted) > 1:
        for term in terms_names_sorted[1:]:
            if len(anded_result) <= len(term_ids_mapping[term]):
                # term_ids_mapping[term] outputs a set of doc ids for "term"
                short = anded_result
                long = term_ids_mapping[term]
            else:
                long = anded_result
                short = term_ids_mapping[term]
            # pre sorting short and long lists makes the search faster
            short = sorted(short)
            long = sorted(long)
            long_index = 0
            for item_in_short in short:
                for l_i in range(long_index, len(long)):
                    if item_in_short == long[l_i]:
                        anded_result.add(item_in_short)
                    long_index += 1

    return query_term_docs, anded_result


def main():
    q_term = 'algorithm big'
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
    print("result length: ", len(ranking_key))
    ranked_score_list = []
    for doc_id in ranking_key:
        ranked_score_list.append((doc_id, weighted_docs_dict[doc_id]))

    print(ranked_score_list)
    print(doc_rank_data)
    print(read_docs(ranking_key, sc))
    sc.stop()

if __name__ == '__main__':
    main()
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
        term_documents = set()
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
            category_term = term    # lemmatized and can have numbers

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
                raw_docs_collections = author_zone.filter(lambda line: term in eval(line)[0])
                docs_collect = raw_docs_collections.collect()
                auth_docs = eval(docs_collect[0])[1]
                # print("author ", auth_docs)
                term_documents.update(auth_docs)
            except:
                pass

        if title_term is not None:
            try:
                raw_docs_collections = title_zone.filter(lambda line: term in eval(line)[0])
                docs_collect = raw_docs_collections.collect()
                title_docs = eval(docs_collect[0])[1]
                # print("title ", title_docs)
                term_documents.update(title_docs)
            except:
                pass

        if publisher_term is not None:
            try:
                raw_docs_collections = publisher_zone.filter(lambda line: term in eval(line)[0])
                docs_collect = raw_docs_collections.collect()
                publisher_docs = eval(docs_collect[0])[1]
                # print("publisher ", publisher_docs)
                term_documents.update(publisher_docs)
            except:
                pass

        if key_words_term is not None:
            try:
                raw_docs_collections = key_words_zone.filter(lambda line: term in eval(line)[0])
                docs_collect = raw_docs_collections.collect()
                key_words_docs = eval(docs_collect[0])[1]
                # print("key_words ", key_words_docs)
                term_documents.update(key_words_docs)
            except:
                pass

        if isbn_term is not None:
            try:
                raw_docs_collections = isbn_zone.filter(lambda line: term in eval(line)[0])
                docs_collect = raw_docs_collections.collect()
                isbn_docs = eval(docs_collect[0])[1]
                # print("isbn ", isbn_docs)
                term_documents.update(isbn_docs)
            except:
                pass

        if category_term is not None:
            try:
                raw_docs_collections = category_zone.filter(lambda line: term in eval(line)[0])
                docs_collect = raw_docs_collections.collect()
                category_docs = eval(docs_collect[0])[1]
                # print("category ", category_docs)
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
    # docs_dict = {'cormen': {'i-bUBQAAQBAJ', 'g8d3PwAACAAJ', '46JvnQEACAAJ', 'AGAiAgAAQBAJ', 'JtlDAAAACAAJ', 'he2koAEACAAJ', 'YsE2pwAACAAJ', 'dEplHQAACAAJ', 'yfnloQEACAAJ', '7cNgPwAACAAJ', 'ZLN9CQAAQBAJ', 'mmFGSwAACAAJ', 'nunPngEACAAJ', 'COxdlgEACAAJ', 'NdSXoAEACAAJ', 'Zl-lcQAACAAJ', 'Jwr8jwEACAAJ', '5kmsSgAACAAJ', 'unDKrQEACAAJ', 'NLngYyWFl_YC', 'm2QwnwEACAAJ', 'Ew--ngEACAAJ', 'pUGYSQAACAAJ', 'jUF9BAAAQBAJ', '3QU2SgAACAAJ', 'z73loAEACAAJ', 'F3anBQAAQBAJ', 'aefUBQAAQBAJ', 'EUrXoAEACAAJ', 'h2xRPgAACAAJ', 'r-oGPLclJc0C', 'HbxILgEACAAJ', '0hX5ygAACAAJ', 'eW84rgEACAAJ', 'U2P3NwAACAAJ'}, 'thomas': {'HKtHIwpxjqIC', 'Vk4nAgAAQBAJ', 'EMcCRIenE_UC', 'YsFMAgAAQBAJ', 'fNC3fdxYgZMC', '3sS9ehOCEC8C', 'agr9VJf59EAC', 'oiKMBQAAQBAJ', 'x0qEAgAAQBAJ', '8blUBAAAQBAJ', 'xMvEIAsuy3gC', 'yA6POMQa_HgC', 'yN84TKB6rXIC', '8Qe2AwAAQBAJ', 'dLTpAwAAQBAJ', 'e1HcVQJE34EC', 'bVVbR1jgARQC', 'hFEnAgAAQBAJ', 'HRr6LXmlVm8C', 'lHAkBQAAQBAJ', '__C0AwAAQBAJ', 'KHEkBQAAQBAJ', '_cEjBQAAQBAJ', 'hQi2AwAAQBAJ', 'Ry64BAAAQBAJ', 'pbaJVzdNMGgC', 'p8gScvl9lucC', '5roYBAAAQBAJ', 'sMJg6nKaLXMC', 'TJQU1i33Fd8C', 'FKVY1ybWQi4C', 'l_ICBAAAQBAJ', 'zajgJy6F_h4C', 'm4iwBAAAQBAJ', '8e7UAAAAQBAJ', '3TCZxMJ_mLAC', 'DiuiCE-WmgEC', '0JW-rQEACAAJ', 'BeSVPDPwf8IC', 'q08VCgAAQBAJ', 'muUp_YW6IikC', 'w1t9oAEACAAJ', 'jK4QyY4vIHgC', '3fG0AwAAQBAJ', 'Lnfr0efDl0gC', '6pWWYFeiQbkC', '0LMeBgAAQBAJ', 'SnLF5AUpucMC', 'gpfEh1slfPUC', 'odWwa3jFzVMC', 'Wh3mVlAjxE8C', 'Mp5FCgAAQBAJ', '55Mf8bRkLHcC', 'Upy8D3Qo8IIC', 'qINa6ZzeYU4C', 'lGGjTCsxYrEC', 'kx2MBQAAQBAJ', '_xhGLwEACAAJ', 'lvK0AwAAQBAJ', 'Svw56Kfkx5gC', 'xkb0KBpKHmsC', '8d54p2PP6VwC', 'D2eDbwAACAAJ', 'VSGMBQAAQBAJ', 'eiGMBQAAQBAJ', 'FZRD4o9e8-4C', 'Cia1AwAAQBAJ', 'vJY-COOkrxIC', 'maaRptUU23UC', 'iu60AwAAQBAJ', 'mbZEaK1p1aUC', 'HAQQQcoXE7cC', 'hwO7S55TUvkC', 'vdgZ9FsLvscC', 'IKTFAgAAQBAJ', 'rXXe9Q_shUcC', 'YAi2AwAAQBAJ', 'iLGwqxw5UlEC', 'KZJRBAAAQBAJ', 'vL0PBgAAQBAJ', 'JX-nCgAAQBAJ', 'KfShAgAAQBAJ', 'A4lwAwAAQBAJ', 'eKG1AwAAQBAJ', 'WZnVoQEACAAJ', 'ta2LBQAAQBAJ', 'h-H3AgAAQBAJ', 'nRSMBQAAQBAJ', 'NUR3CAAAQBAJ', '3XrAoznss68C', 'tiUeAwAAQBAJ', '9H_zBgAAQBAJ', 'NKY37Flu9GcC', 'KvO0AwAAQBAJ', '7jzFAgAAQBAJ', 'Sn-nCgAAQBAJ', 'XHfYYW5uDSAC', '1yqRBQAAQBAJ', 'TBIlXe0M2UkC', 'Th1dCgFYvo4C', 'BJmNdRxGho0C', 'kRUz8yq4HJUC', '7mp-AgAAQBAJ', '1N8katBAbJoC', '0Nh0CgAAQBAJ', 'aMjehs6iXoQC', 'm4Zozc-14VwC', 'h3x1nVfkpaAC', 'COxdlgEACAAJ', 'UD663DTHewUC', 'LqG1AwAAQBAJ', '8rtVcpqOhGQC', '6mzeuYfqrp0C', 'aXd_tjdwb84C', 'qqtLbwAACAAJ', '2Wzni9OuPNMC', 'wTLvchzY9eQC', 'XPGCtgAACAAJ', 'gQnb2ewvlLYC', 'HfDUAAAAQBAJ', 'gB2NBgAAQBAJ', 'W7X4jgvvdikC', 'r4jlBgAAQBAJ', 'eg8lkuT0vV4C', 'ElSNcpqGOw0C', 'hK4Ew-fXRagC', 'wRYvCvkhwmAC', '5s3ICxgCuiAC', 'WxVTBAAAQBAJ', 'LQlQBAAAQBAJ', 'GqPIPvCbAv0C', '9kI6bAkYx0oC', 'Ei2FCC1v7PkC', 'LNojrgEACAAJ', 'MROAl7JUgHQC', 'L2LrAgAAQBAJ', 'dweQK8BSgPoC', 'MazDUeO5UzIC', 'Ph04BAAAQBAJ', 'ngUcvG7RmIoC', 'ZRqMBQAAQBAJ', '5OfWrQEACAAJ', 'PhrFAgAAQBAJ', 'iv6Uth4ZxMsC', 'v46Z1hUcud8C', 'KOrHoQEACAAJ', 'bmxAYgEACAAJ', 'b_ECBAAAQBAJ', 'cYUzRMFkVHUC', '5LUF-AUIz2kC', 'lBuZSQAACAAJ', 'A3EkBQAAQBAJ', 'ZFAVCgAAQBAJ', 'Gvj-oAEACAAJ', '7mOOBAAAQBAJ', 'y8caBQAAQBAJ', '37HI2RpU1jMC', '6BBwAwAAQBAJ', 'S3wSTfB65c8C', '3Q0930ersqwC', 'zBGm6vxcI4cC', '3PwHHSrPJfUC', 'y1fDCAAAQBAJ', '5g-94jmpmVAC', 'kRyMBQAAQBAJ', 'EjMS0oZJQ-wC', 'Hw4gWVAoz8kC', 'UcqxCAAAQBAJ', 'cCZ2kJmL1pIC', 'emHWx_0DDOcC', 'vh-MBQAAQBAJ', 'Sh2MBQAAQBAJ', 'Wh5XTdZFUCwC', '4EccO3iAB-8C', 'p2EMm24WqVYC', 'w1_LeeZkKAIC', 'gNcgvoypGo0C', 'i03oBAAAQBAJ', 'iboi4uBNycwC', 'NwrdBE45GTYC', 'RDd1ifhdiZEC', '508_hiGO5WMC', 'CUhWCgAAQBAJ', 'C-nEHgDyfGwC', 'S1RNBAAAQBAJ', 'zD3FAgAAQBAJ', 'tTSOAwAAQBAJ', 'PcgUuR9KoVYC', 'yYISw61HPyUC', '7l-3AwAAQBAJ', 'dYqVAhKfLakC', 'PoU8X71uDYkC', 'gKMql-HcnUYC', 'n1qaOUDfCeEC', 'ExUPpuqhTjUC', 'x0reoAEACAAJ', '2VD_BgAAQBAJ', '8FfgtnsvTRAC', 'ZuT7oAEACAAJ', 'K0H1mmJZ24sC', 'rfLCAwAAQBAJ', 'itz6rQEACAAJ', 'Dd7VrQEACAAJ', 'yqlT1QLq4K0C', 'bjv2nQEACAAJ', '7XUkBQAAQBAJ', 'wjWmoAEACAAJ', 'eSdMaF7ONyEC', 'ADdaMuROihQC', 'ZkZlpYjJIi4C', '9wbsngH6C8kC', 'HBvFAgAAQBAJ', 'fDqQBQAAQBAJ', 'VRe0AwAAQBAJ', 'SQUMAQAAQBAJ', 'Vcc0AAAAQBAJ', 'mCfqCAAAQBAJ', 'ZzQ3BAAAQBAJ', '65h7BAAAQBAJ', '8vi0AwAAQBAJ', 'ptjqAYi4GdwC', 'WzbUevKfXH4C', '7AHSTeFtgOMC', 'eZIToh9HlwUC', 'MoYosRUL4QcC', 'PeH3AgAAQBAJ', 'xLP3sa-bnkoC', 'baagBgAAQBAJ', 'AGAiAgAAQBAJ', 'Bkl3BQZimO0C', 'Ew_P1mylvJQC', 'AqLDrQEACAAJ', '-JkP_OPXgaQC', '2y5oAgAAQBAJ', 'zR2TAAAACAAJ', '7aaTBQAAQBAJ', 'Qg9o6G-EDwsC', 'izOvRWOzKgEC', 'Fg81NnVsY3sC', '2aNkRSrkfP8C', 'V4IQH8Tqr4sC', 'NyTEWziny_IC', 'wXOaOXxQ3bcC', 'UNbMBgAAQBAJ', 'HmZe35wO9_gC', 'Uy6CBwAAQBAJ', 'jNu3XyA0yz0C', 'cS8BywAACAAJ', 'nGWBBgAAQBAJ', '3mVevn3NWYAC', 'aBVXkYm8C-AC', 'cLFvBQAAQBAJ', 'RjdBAwAAQBAJ', 'pSGHSQAACAAJ', 'z0aUEPNxDVMC', 'mTnG1DV-_PEC', 'jt59AwAAQBAJ', '7eIO4CuUZ94C', 'kwA8dk_L5w4C', 'IakVOAAACAAJ', 'WMN7AgAAQBAJ', '9fUBwrjY3E0C', 'bvZ-W9AMRpMC', 'U0Z1a80-UtIC', 'WU7wrQEACAAJ', 'EoF9BgAAQBAJ', 'cR6MBQAAQBAJ', 'P7OVYI9Gy6sC', '-RqMBQAAQBAJ', 'CqCvCAAAQBAJ', 'IVUnAgAAQBAJ', 'QzQ4AwAAQBAJ', 'ocT4oAEACAAJ', 'imftctLz1PAC', 'xZMAADFmJSoC', 'YvQcBQAAQBAJ', 'oM2FRyqFxkwC', 'msFsx6XtNXAC', 'gLicAgAAQBAJ', '74rlm2k97s0C', 'iS43zLZH1YUC', 'wbwAz2HXhacC', 'ul0kFIxtMfkC', 'lZGDQBeLqD0C', 'IOxYYlAPPGQC', 'yiOMBQAAQBAJ', 'Hj0GCgAAQBAJ'}}
    print(docs_dict)
    for k, v in docs_dict.items():
        print(k , " ", len(v))
    terms_sorted = sorted(docs_dict, key=lambda doc_list_key: len(docs_dict[doc_list_key]))

    # buffer_set = {}
    # for doc_id_list in docs_dict.values():
    #     for doc_id in doc_id_list:
    #         buffer_set[doc_id] = True
    # for doc_id_list in docs_dict.values():
    #     for doc_id in buffer_set:
    #         if doc_id not in doc_id_list:
    #             buffer_set[doc_id] = False
    # for doc_id, status in buffer_set.items():
    #     if status:
    #         print(doc_id)
    print(terms_sorted)
    previous_computed_doc_ids = terms_sorted[0]
    for term in terms_sorted[1:]:
        next_doc_ids = set()
        if len(docs_dict[previous_computed_doc_ids]) <= len(docs_dict[term]):
            short = docs_dict[previous_computed_doc_ids]
            long  = docs_dict[term]
        else:
            long = docs_dict[previous_computed_doc_ids]
            short = docs_dict[term]
        for item_in_short in short:
            if item_in_short in long:
                next_doc_ids.add(item_in_short)
        previous_computed_doc_ids = next_doc_ids

    print(previous_computed_doc_ids)

    sc.stop()

if __name__ == '__main__':
    main()
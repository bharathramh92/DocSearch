ISBN_10 = "ISBN_10"
ISBN_13 = "ISBN_13"
CATEGORIES = "categories"
AUTHORS = "authors"
TITLE = "title"
PUBLISHER = "publisher"
KEYWORDS = "keyWords"
VIEWS = "views"
ENTITIES = [ISBN_10, ISBN_13, CATEGORIES, AUTHORS, TITLE, PUBLISHER, KEYWORDS]
LEMMA_ENTITIES = [TITLE, CATEGORIES, KEYWORDS]
MODEL_WEIGHTS = {ISBN_10: .3, ISBN_13: .3, AUTHORS: .18, PUBLISHER: .15, TITLE: .16, CATEGORIES: .12,
                   KEYWORDS: .1, VIEWS: .5}


VIEW_RANKING_MAX_DOCS = 20


def best_zone(ls):
    return sorted(ls, key=lambda zone: MODEL_WEIGHTS[zone], reverse=True)[0]
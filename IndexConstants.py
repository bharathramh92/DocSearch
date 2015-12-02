ISBN_10 = "ISBN_10"
ISBN_13 = "ISBN_13"
CATEGORIES = "categories"
AUTHORS = "authors"
TITLE = "title"
PUBLISHER = "publisher"
KEYWORDS = "keyWords"

ENTITIES = [ISBN_10, ISBN_13, CATEGORIES, AUTHORS, TITLE, PUBLISHER, KEYWORDS]
LEMMA_ENTITIES = [TITLE, CATEGORIES, KEYWORDS]
MODEL_WEIGHTS = {ISBN_10: .3, ISBN_13: .3, AUTHORS: .18, PUBLISHER: .15, TITLE: .15, CATEGORIES: .12,
                   KEYWORDS: .1}
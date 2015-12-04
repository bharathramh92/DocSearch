Sub projects (README of its project is inside their corresponding directory)
- InitialDataExtraction(Data retrieval from Google Books API).
- KeyWord(KeyWord generation using Stanford NLP)

Indexing/Querying
- Query.py and InvertedIndex.py are main files for indexing and querying.

Resources requirement for indexing/querying
- For indexing, Resource/id_doc_rdd_raw directory needs to be created in this path were README.md resides.
- Raw data id_doc_rdd_raw which maps doc_id to document data would be the data structure.
- id_doc_rdd_raw eg: {"uKQ0CgAAQBAJ": {"imageLinks": {"smallThumbnail": "http://books.google.com/books/content?id=uKQ0CgAAQBAJ&printsec=frontcover&img=1&zoom=5&edge=curl&so
urce=gbs_api", "thumbnail": "http://books.google.com/books/content?id=uKQ0CgAAQBAJ&printsec=frontcover&img=1&zoom=1&edge=curl&source=gbs_api"}, "catego
ries": ["Fiction"], "description": "Meredith has never considered herself submissive even though her greatest fantasy is being pleasured against her wi
ll. When Mark orders her to her knees the first time, she can\u2019t get there fast enough\u2014and then hates herself afterward for losing control. A"
, "publisher": "Ellora's Cave Publishing Inc", "ISBN_13": "9781419994289", "keyWords": ["Meredith", "Mark"], "infoLink": "http://books.google.com/books
?id=uKQ0CgAAQBAJ&dq=go&as_pt=BOOKS&hl=&source=gbs_api", "authors": ["L.E. Chamberlin"], "ISBN_10": "141999428X", "maturityRating": "NOT_MATURE", "title
": "The Rewards of Letting Go"}}

- First run InvertedIndex.py to create index_rdd.
- index_rdd map each indexed term to corresponding documend id with the zone/entity name
- index_rdd eg: ('aceline', (('K3NLoAEACAAJ', 'keyWords'), ('7DiNBwAAQBAJ', 'title')))
- 'aceline' is the indexed term, K3NLoAEACAAJ is the doc_id, keyWords was the zone and likewise.

- view_rdd can be used if using ranking based on view count has to be considered as well
- view_rdd eg: ('NLngYyWFl_YC', 1292215), where the data format is (doc_id, view_count)

Query.py --> Query for document from indexed data
- get_docs(query_term, zone_restriction) method has to be called as shown in main() method.
- Only one the parameters has to be defined
- q_term is used for searching the keyword across all zones
- zone_restriction is used if particular terms have to be restricted to search within a zone.
- query_term: search term if it has to be searched across all the zones.
- zone_restriction: dictionary whose keys are zone and the corresponding value is the search term of that zone.
- eg: zone_restriction = {KEYWORDS: 'pop', CATEGORIES: 'art', TITLE: 'culture', PUBLISHER: 'macmillan'}
- eg: q_term = 'cormen clrs'
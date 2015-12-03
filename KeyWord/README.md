Project details
- KeyWord sub project is retrieve keywords from description of books
- Tried two approaches, Stanford Named Entity Recognition(NER) and Proper Noun extraction
- Keywords from NER's found to be unreliable and Proper Nouns gave better results.
- Issues with NER's were
    - Many unwanted words
    - Improper entity definition for the terms and hence unreliable
- Proper Noun are not giving False Positives as in the former case

- Json data manipulation was performed using gson library

stopWords.txt
- List houses all stop words which were to be removed

Main.java
- src for the keyword extraction using NER and Proper Nouns (Only the later method is called for output).


Book.java
- An object class for gson data dumping/loading

StopWordRemoval
- Helper class for stop words removal
import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.ling.TaggedWord;
import edu.stanford.nlp.tagger.maxent.MaxentTagger;

import java.io.*;
import java.util.*;

public class Main {
    static MaxentTagger tagger;
    public static void main(String[] args) throws IOException {
        tagger = new MaxentTagger(getModeldir() + "taggers/english-left3words-distsim.tagger");
        Utils utils = new Utils(args[0]);
        HashMap<Integer, String> descriptions = utils.getDescriptions();
//        int lastIndex = 100;
//        if(descriptions.size() < 100)
        int lastIndex = descriptions.size();

        for(int i=0; i < lastIndex; i++){
            String[] keyWords = null;
            System.out.println("Completed "+ (float)i/lastIndex*100 + "%");
//            String[] keyWords = stanfordNER(descriptions.get(i));
//            Arrays.asList(keyWords).forEach(System.out::println);
            if(descriptions.containsKey(i)) {
                keyWords = usingStanfordMaxentPOS(descriptions.get(i));
                utils.updateKeyWords(keyWords, i);
            }
//            Arrays.asList(keyWords).forEach(System.out::println);
        }
        tagger = null;
        descriptions.clear();
        utils.writeKeyWords();
    }

    public static String[] stanfordNER(String description) throws IOException {
//        System.out.println("description = " + description);
        HashSet<String> hSet = new HashSet<>();
        String model = getModeldir() + "stanford-ner-2012-11-11-nodistsim/english.all.3class.nodistsim.crf.ser.gz";
        CRFClassifier<CoreLabel> classifier = CRFClassifier.getClassifierNoExceptions(model);
        List<List<CoreLabel>> entityList = classifier.classify(description);
        for(List<CoreLabel> entity: entityList) {
            for(CoreLabel coreLabel: entity){
                String word = coreLabel.word();
                String label = coreLabel.get(CoreAnnotations.AnswerAnnotation.class);
                if(!"O".contains(label) && !StopWordRemoval.isValid(word)) {
//                    System.out.println("label = " + label+ " word = "+ word);
                    hSet.add(word.toLowerCase());
                }
            }
        }
        return hSet.toArray(new String[hSet.size()]);
    }

    private static String[] usingStanfordMaxentPOS(String description) {
        HashSet<String> hSet = new HashSet<>();
        try {

            List<List<HasWord>> sentences = MaxentTagger.tokenizeText(new StringReader(description));
            for (List<HasWord> sentence : sentences) {
                List<TaggedWord> taggedSentence = tagger.tagSentence(sentence);
                for (TaggedWord taggedWord : taggedSentence) {
                    if (taggedWord.tag().startsWith("NNP") ||  taggedWord.tag().startsWith("NNPS")) {
                        if (taggedWord.word().split("[^a-zA-Z\\-]+").length == 1) {
                            hSet.add(taggedWord.word());
                        }
                    }
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return hSet.toArray(new String[hSet.size()]);
    }

    public static String getModeldir(){
        return "/media/Data/Dropbox/PyCharm_Projects/DocSearch/KeyWord/libs/";
    }
}

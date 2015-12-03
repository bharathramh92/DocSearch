import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;

public class StopWordRemoval {
    static private HashSet<String> stopWordList;

    public static void loadData() throws IOException {
        BufferedReader bf = new BufferedReader(new FileReader("KeyWord/src/stopWords.txt"));
        String line;
        stopWordList = new HashSet<>();
        while((line = bf.readLine()) != null){
            stopWordList.add(line);
        }
    }

    public static boolean isValid(String word) throws IOException {
        loadData();
        word = word.toLowerCase();
        String regex = "[^a-zA-Z\\-]+";
//        System.out.println(word + " " + (word.split(regex).length != 1));
        String[] splitWord = word.split(regex);
        return splitWord.length != 1 || stopWordList.contains(word) || !splitWord[0].equals(word) || word.length() <= 2;
    }
}

import com.google.gson.*;
import com.google.gson.stream.JsonWriter;

import java.io.*;
import java.lang.reflect.Type;
import java.util.*;

public class Utils {

    JsonArray root;
    Book[] books;

    public Utils(String fileName) throws IOException {
        BufferedReader bf = new BufferedReader(new FileReader(fileName));
        String data = bf.readLine();
        books = new Gson().fromJson(data, Book[].class);
        bf.close();
    }

    public HashMap<Integer, String> getDescriptions(){

        HashMap<Integer, String> descriptions = new HashMap<Integer, String>();
        for(int i=0; i<books.length; ++i){
            Book item = books[i];
            if(item.getDescription() != null){
                descriptions.put(i, item.getDescription());
            }
        }
        return descriptions;
    }

    public void writeKeyWords(){
        try {

            JsonWriter writer = new JsonWriter(new OutputStreamWriter(new FileOutputStream("data_keywords.json"), "UTF-8"));
            writer.setIndent("  ");
            writer.beginArray();
            Gson gson = new Gson();
            for (Book message : books) {
                gson.toJson(message, Book.class, writer);
            }
            writer.endArray();
            writer.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void updateKeyWords(String[] keyWords, int index){
        books[index].setKeyWords(keyWords);
    }
}

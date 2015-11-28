import java.util.HashMap;

/**
 * Created by bharath on 11/28/15.
 */
public class Book {
    String description, title, ISBN_13, maturityRating, ISBN_10, id, pageCount, infoLink;
    String publisher;
    String[] authors, keyWords;

    public String getDescription() {
        return description;
    }

    public void setKeyWords(String[] keyWords) {
        this.keyWords = keyWords;
    }

    ImageLinks imageLinks;

    public Book(){
        imageLinks = new ImageLinks();
    }

    public class ImageLinks {
        String thumbnail, smallThumbnail;
    }

}
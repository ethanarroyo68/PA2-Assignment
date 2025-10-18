package ngram;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Article {
    private String title, articleId, bodyText;
    private boolean valid;

    public Article(String rawText) {
        String[] parts = rawText.split("<====>");
        if (parts.length < 3 || parts[2].length() <= 0) {
            valid = false;
            return;
        }

        this.title = parts[0].trim();
        this.articleId = parts[1].trim();
        this.bodyText = formatArticle(parts[2]);
        valid = true;

    }

    public Boolean isValid() {
        return valid;
    }

    public String title() {
        return title;
    }

    public String getArticleId() {
        return articleId;
    }

    public String getArticleBody() {
        return bodyText;
    }

    private String formatArticle(String articleText) {

        articleText = articleText.toLowerCase();
        return articleText.replaceAll("[^a-z\\s-]", "");

    }
}
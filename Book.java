package ngram;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Book {
	private String headerText, bodyText, author, year;
	private int ngramCount;

	public Book(String rawText, int ngramCount) {
		this.ngramCount = ngramCount;
		String[] parts = rawText.split("\\*\\*\\*");
		headerText = parts.length > 0 ? parts[0] : "";
		bodyText = parts.length > 2 ? formatBook(parts[2]) : "";
		author = parseAuthor(headerText);
		year = parseYear(headerText);
	}

	private String parseAuthor(String headerText) {
		if (headerText == null || headerText.isEmpty()) {
			return "Unknown";
		}
		Pattern namePattern = Pattern.compile("Author:(.*)");
		Matcher nameMatcher = namePattern.matcher(headerText);
		String nameline = nameMatcher.find() ? nameMatcher.group(0) : "";
		if (nameline.isEmpty()) {
			return "Unknown";
		}
		String[] namelineSplit = nameline.split(" ");
		int n = namelineSplit.length - 1;
		while (n >= 1) {
			if (namelineSplit[n].matches("[a-zA-Z]+")) {
				return namelineSplit[n].toLowerCase();
			}
			n--;
		}
		return "Unknown";
	}

	private String parseYear(String headerText) {
		if (headerText == null || headerText.isEmpty()) {
			return "Unknown";
		}
		Pattern datePattern = Pattern.compile("Release Date:(.*)");
		Matcher dateMatcher = datePattern.matcher(headerText);
		String date = dateMatcher.find() ? dateMatcher.group(0) : "";
		if (date.isEmpty()) {
			return "Unknown";
		}
		Pattern yearPattern = Pattern.compile("\\d{4}");
		Matcher yearMatcher = yearPattern.matcher(date);
		return yearMatcher.find() ? yearMatcher.group(0) : "Unknown";
	}

	public String getBookAuthor() {
		return author;
	}

	public String getBookYear() {
		return year;
	}

	public String getBookHeader() {
		return headerText;
	}

	public String getBookBody() {
		return bodyText;
	}

	private String formatBook(String bookText) {

		if (ngramCount < 2) {
			// ##: Format book text for unigram
			// Hint: Consider case, punctuation, and special characters
			bookText = bookText.toLowerCase();
			return bookText.replaceAll("[^a-z\\s-]", "");

		} else {
			// ##: Format book text for bigram
			// Hint: Consider sentence boundaries in addition to unigram formatting
			bookText = bookText.toLowerCase();
			return bookText.replaceAll("[^a-z\\s\\-!.?]", "");

		}
	}
}

package ngram;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.UUID;

/**
 * Moved out of NgramMapReduce as a top-level class.
 * Emits Text keys and VolumeWriteable values.
 */
public class TokenizerMapper extends Mapper<Object, BytesWritable, Text, VolumeWriteable> {

    private static String cleanToken(String token) {
        if (token.equals("_START_") || token.equals("_END_"))
            return token;
        return token.replaceAll("[^\\p{L}\\p{N}_\\t \\ -]", "")
                .replaceAll("-{2,}", "-")
                .replaceAll("-+\\t", "\\t")
                .replaceAll("^-+|-+$", "")
                .toLowerCase()
                .trim();
    }

    @Override
    public void map(Object key, BytesWritable bWriteable, Context context)
            throws IOException, InterruptedException {
        NgramMapReduce.Profiles profile = context.getConfiguration().getEnum("profile", NgramMapReduce.Profiles.A1);

        String rawText = new String(bWriteable.getBytes());
        Book book = new Book(rawText, profile.ngramNum);
        StringTokenizer itr = new StringTokenizer(book.getBookBody());

        String previousToken = "_START_";
        UUID bookId = UUID.randomUUID();

        while (itr.hasMoreTokens()) {
            String currentToken = cleanToken(itr.nextToken());
            if (currentToken.isEmpty() || currentToken.matches("\\d+"))
                continue;

            String tokenKey = "";
            String endKey = "";
            boolean end = false;

            switch (profile) {
                case A1:
                    tokenKey = currentToken + "\\t" + book.getBookYear();
                    break;

                case B1:
                    tokenKey = currentToken + "\\t" + book.getBookAuthor();
                    break;

                case A2:
                case B2:
                    tokenKey = cleanToken(previousToken) + " " + currentToken;
                    if (currentToken.endsWith(".") || currentToken.endsWith("!") || currentToken.endsWith("?")) {
                        previousToken = "_START_";
                        end = true;
                        endKey = currentToken + " _END_";
                    } else {
                        end = false;
                        previousToken = currentToken;
                    }
                    if (profile == NgramMapReduce.Profiles.A2) {
                        tokenKey = tokenKey + "\\t" + book.getBookYear();
                        endKey = endKey + "\\t" + book.getBookYear();
                    } else {
                        tokenKey = tokenKey + "\\t" + book.getBookAuthor();
                        endKey = endKey + "\\t" + book.getBookAuthor();
                    }
                    break;

                default:
                    tokenKey = currentToken;
                    break;
            }

            if (tokenKey.isEmpty() || tokenKey.matches("\\d+\\t.*"))
                continue;
            if (end && (endKey.isEmpty() || endKey.matches("\\d+\\t.*") || endKey.startsWith("_END_"))) {
                end = false;
            }

            VolumeWriteable volWrite = new VolumeWriteable();
            MapWritable map = new MapWritable();
            map.put(new Text(bookId.toString()), new IntWritable(1));
            volWrite.set(map, new IntWritable(1));

            if (end)
                context.write(new Text(endKey), volWrite);
            context.write(new Text(tokenKey), volWrite);
        }
    }
}
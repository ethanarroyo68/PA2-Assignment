package ngram;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.UUID;

/**
 * RawFrequencyMapper for ProfileA Job1 (raw frequency).
 * Emits token keys and VolumeWriteable values.
 * This was requested to be a standalone mapper for ProfileA.
 */
public class RawFrequencyMapper extends Mapper<Object, BytesWritable, Text, VolumeWriteable> {

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
        // For ProfileA (raw frequency), we will emit unigram + year as the key.
        String rawText = new String(bWriteable.getBytes());
        Book book = new Book(rawText, 1); // unigram handling
        StringTokenizer itr = new StringTokenizer(book.getBookBody());

        UUID bookId = UUID.randomUUID();

        while (itr.hasMoreTokens()) {
            String currentToken = cleanToken(itr.nextToken());
            if (currentToken.isEmpty() || currentToken.matches("\\d+"))
                continue;

            String tokenKey = currentToken + "\\t" + book.getBookYear();

            VolumeWriteable volWrite = new VolumeWriteable();
            MapWritable map = new MapWritable();
            map.put(new Text(bookId.toString()), new IntWritable(1));
            volWrite.set(map, new IntWritable(1));

            context.write(new Text(tokenKey), volWrite);
        }
    }
}
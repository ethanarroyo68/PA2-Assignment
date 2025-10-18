package ngram;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RawFrequencyMapper extends Mapper<Object, BytesWritable, Text, VolumeWriteable> {

    public void map(Object key, BytesWritable bWriteable, Context context)
            throws IOException, InterruptedException {

        String[] rawText = new String(bWriteable.getBytes(), 0, bWriteable.getLength()).split("\\n\\n+");

        for (String entry : rawText) {
            Article article = new Article(entry);
            if (!article.isValid())
                continue;

            StringTokenizer itr = new StringTokenizer(article.getArticleBody());

            while (itr.hasMoreTokens()) {
                String tokenKey = itr.nextToken();

                tokenKey = tokenKey.replaceAll("-{2,}", "-")
                        .replaceAll("-+\\t", "\\t")
                        .replaceAll("^-+|-+$", "");

                if (tokenKey.isEmpty())
                    continue;

                MapWritable map = new MapWritable();
                map.put(new Text(tokenKey), new IntWritable(1));

                VolumeWriteable volWrite = new VolumeWriteable();
                volWrite.set(map, new IntWritable(1));

                context.write(new Text(article.getArticleId()), volWrite);
            }
        }
    }
}
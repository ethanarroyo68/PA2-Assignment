package ngram;

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class RawFrequencyReducer extends Reducer<Text, VolumeWriteable, Text, Text> {

	public void reduce(Text key, Iterable<VolumeWriteable> values, Context context)
			throws IOException, InterruptedException {

		int mergedCount = 0;
		MapWritable mergedMap = new MapWritable();

		for (VolumeWriteable volWrite : values) {
			mergedCount += volWrite.getCount().get();

			for (Map.Entry<Writable, Writable> entry : volWrite.getVolumeIds().entrySet()) {
				Text unigram = (Text) entry.getKey();
				IntWritable count = (IntWritable) entry.getValue();

				if (mergedMap.containsKey(unigram)) {
					IntWritable existing = (IntWritable) mergedMap.get(unigram);
					mergedMap.put(unigram, new IntWritable(existing.get() + count.get()));
				} else {
					mergedMap.put(unigram, new IntWritable(count.get()));
				}
			}
		}

		VolumeWriteable aggregatedValue = new VolumeWriteable();
		aggregatedValue.set(mergedMap, new IntWritable(mergedCount));

		for (String line : aggregatedValue.unigramString().split("\\n")) {
			context.write(key, new Text(line));
		}
	}
}

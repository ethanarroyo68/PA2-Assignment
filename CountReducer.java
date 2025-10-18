package ngram;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.io.IOException;
import java.util.Map;

/**
 * Reducer that aggregates VolumeWriteable values. Moved out from NgramMapReduce.
 */
public class CountReducer extends Reducer<Text, VolumeWriteable, Text, VolumeWriteable> {

    @Override
    public void reduce(Text key, Iterable<VolumeWriteable> values, Context context)
            throws IOException, InterruptedException {
        int mergedCount = 0;
        MapWritable mergedMap = new MapWritable();

        for (VolumeWriteable volWrite : values) {
            mergedCount += volWrite.getCount().get();

            for (Map.Entry<Writable, Writable> entry : volWrite.getVolumeIds().entrySet()) {
                mergedMap.put(entry.getKey(), entry.getValue());
            }
        }

        VolumeWriteable aggregatedValue = new VolumeWriteable();
        aggregatedValue.set(mergedMap, new IntWritable(mergedCount));

        context.write(key, aggregatedValue);
    }
}
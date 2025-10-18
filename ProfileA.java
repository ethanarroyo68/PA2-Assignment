package ngram;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.ToolRunner;

// Custom imports from PA1
import ngram.WholeFileInputFormat;
import ngram.VolumeWriteable;

public class ProfileA /* extends Configured implements Tool */ {

    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "ProfileA_Job1_RawFrequency");

        job1.setJarByClass(ProfileA.class);
        job1.setInputFormatClass(WholeFileInputFormat.class);

        job1.setMapperClass(RawFrequencyMapper.class);
        job1.setReducerClass(RawFrequencyReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(VolumeWriteable.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.waitForCompletion(true);

        // -----------------------------------
        // 3. Job 2 — Term Frequency
        // - Input = Job 1 output
        // - Calculates TF for each unigram using normalized formula
        // - Mapper emits (docID, unigram TF)
        // Output: <docID, (unigram TF)>
        // -----------------------------------
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "ProfileA_Job2_TermFrequency");

        // (Setup will mirror Job 1, but with TF Mapper/Reducer)
        // FileInputFormat.addInputPath(job2, previous_output_path);
        // FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        // -----------------------------------
        // 4. Job 3 — TF-IDF
        // - Input = Job 2 output
        // - Uses counters to get total document count N
        // - Calculates IDF and multiplies by TF
        // Output: <docID, (unigram TF-IDF value)>
        // -----------------------------------
        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3, "ProfileA_Job3_TFIDF");

        // (Setup similar to Job 2)
        // job3.getConfiguration().setLong(..., ...); // For counter passing
        // FileInputFormat.addInputPath(job3, new Path(args[3]));
        // FileOutputFormat.setOutputPath(job3, new Path(args[4]));

        // -----------------------------------
        // 5. Chain Jobs Together
        // - Run Job 1 → Job 2 → Job 3 sequentially
        // - Exit if any job fails
        // -----------------------------------
        // if (!job2.waitForCompletion(true)) System.exit(1);
        // if (!job3.waitForCompletion(true)) System.exit(1);

        // System.exit(0);
    }
}

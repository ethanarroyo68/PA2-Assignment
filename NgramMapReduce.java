package ngram;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;

import ngram.WholeFileInputFormat;
import ngram.VolumeWriteable;

/**
 * NgramMapReduce: keep only the driver logic here.
 * Mapper/Reducer/other MapReduce helper classes have been moved to top-level files:
 * - TokenizerMapper.java
 * - CountReducer.java
 *
 * Combiner/Reducer set to CountReducer.class and Mapper set to TokenizerMapper.class
 */
public class NgramMapReduce extends Configured implements Tool {
	public static enum Profiles {
		A1('a', 1),
		B1('b', 1),
		A2('a', 2),
		B2('b', 2);

		private final char profileChar;
		private final int ngramNum;

		private Profiles(char c, int n) {
			profileChar = c;
			ngramNum = n;
		}

		public boolean equals(Profiles p) {
			return (this.ngramNum == p.ngramNum) && (this.profileChar == p.profileChar);
		}
	}

	public static int runJob(Configuration conf, String inputDir, String outputDir) throws Exception {
		// function to run job
		Job job = Job.getInstance(conf, "ngram");

		// specify classes for Map Reduce tasks
		job.setInputFormatClass(WholeFileInputFormat.class);
		job.setJarByClass(NgramMapReduce.class);

		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(CountReducer.class);
		job.setReducerClass(CountReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(VolumeWriteable.class);

		FileInputFormat.addInputPath(job, new Path(inputDir));
		FileOutputFormat.setOutputPath(job, new Path(outputDir));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new NgramMapReduce(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Profiles profiles[] = { Profiles.A1, Profiles.A2, Profiles.B1, Profiles.B2 };
		for (Profiles p : profiles) {
			conf.setEnum("profile", p);
			System.out.println("For profile: " + p.toString());
			if (runJob(conf, args[0], args[1] + p.toString()) != 0)
				return 1; // error
		}
		return 0; // success
	}
}
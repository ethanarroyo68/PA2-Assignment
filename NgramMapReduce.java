package ngram;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import ngram.VolumeWriteable;
import ngram.Book;
import ngram.WholeFileInputFormat;

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

	public static class TokenizerMapper extends Mapper<Object, BytesWritable, Text, VolumeWriteable> {

		private static String cleanToken(String token) {
			if (token.equals("_START_") || token.equals("_END_"))
				return token;
			return token.replaceAll("[^\\p{L}\\p{N}_\\t \\-]", "")
					.replaceAll("-{2,}", "-")
					.replaceAll("-+\\t", "\\t")
					.replaceAll("^-+|-+$", "")
					.toLowerCase()
					.trim();
		}

		public void map(Object key, BytesWritable bWriteable, Context context)
				throws IOException, InterruptedException {
			Profiles profile = context.getConfiguration().getEnum("profile", Profiles.A1);

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
						tokenKey = currentToken + "\t" + book.getBookYear();
						break;

					case B1:
						tokenKey = currentToken + "\t" + book.getBookAuthor();
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
						if (profile == Profiles.A2) {
							tokenKey = tokenKey + "\t" + book.getBookYear();
							endKey = endKey + "\t" + book.getBookYear();
						} else {
							tokenKey = tokenKey + "\t" + book.getBookAuthor();
							endKey = endKey + "\t" + book.getBookAuthor();
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

	public static class IntSumReducer extends Reducer<Text, VolumeWriteable, Text, VolumeWriteable> {
		// #TODO#: Initialize any necessary class variables

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

	public static int runJob(Configuration conf, String inputDir, String outputDir) throws Exception {
		// function to run job

		Job job = Job.getInstance(conf, "ngram");

		// specify classes for Map Reduce tasks
		// #TODO#: Replace SPECIFYCLASS.class placeholders with appropriate class names

		job.setInputFormatClass(WholeFileInputFormat.class);
		job.setJarByClass(NgramMapReduce.class);

		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(VolumeWriteable.class);

		FileInputFormat.addInputPath(job, new Path(inputDir));
		FileOutputFormat.setOutputPath(job, new Path(outputDir));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		// ToolRunner allows for command line configuration parameters - suitable for
		// shifting between local job and yarn
		// example command: hadoop jar <path_to_jar.jar> <main_class> -D param=value
		// <input_path> <output_path>
		// We use -D mapreduce.framework.name=<value> where <value>=local means the job
		// is run locally and <value>=yarn means using YARN
		int res = ToolRunner.run(new Configuration(), new NgramMapReduce(), args);
		System.exit(res); // res will be 0 if all tasks are executed succesfully and 1 otherwise
	}

	@Override
	public int run(String[] args) throws Exception {
		// #TODO#: Update the following section
		Configuration conf = this.getConf();
		Profiles profiles[] = { Profiles.A1, Profiles.A2, Profiles.B1, Profiles.B2 };
		for (Profiles p : profiles) {
			conf.setEnum("profile", p); // #TODO#: Set the correct argument in the configuration
			System.out.println("For profile: " + p.toString());
			if (runJob(conf, args[0], args[1] + p.toString()) != 0) // #TODO#: Call runJob with the correct
																	// arguments
				return 1; // error
		}
		return 0; // success
	}
}
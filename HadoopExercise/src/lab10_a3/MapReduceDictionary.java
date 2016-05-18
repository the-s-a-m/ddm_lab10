package lab10_a3;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MapReduceDictionary extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new MapReduceDictionary(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <in> <out>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;

		}
		Job job = Job.getInstance(getConf(), "minimapredwithdefaults");
		job.setJarByClass(this.getClass());

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setMapperClass(DictionaryMapper.class);
		//set output key
		job.setMapOutputKeyClass(Text.class);
		//set output value
		job.setMapOutputValueClass(Text.class);

		job.setPartitionerClass(HashPartitioner.class);

		// job.setNumReduceTasks(0);
		job.setReducerClass(DictionaryReducer.class);
		//set output key
		job.setOutputKeyClass(Text.class);
		//set output value
		job.setOutputValueClass(Text.class);

		job.setOutputFormatClass(TextOutputFormat.class);

		// delete output if existing
		FileSystem fs = FileSystem.get(job.getConfiguration());
		fs.delete(new Path(args[1]), true);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * Creates a mapper with converts Text input to key: Text and DoubleWritable
	 */
	public static class DictionaryMapper extends
			Mapper<Text, Text, Text, Text> {

		/**
		 * Creates a new mapping for the given text.
		 */
		@Override
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {

			String from = key.toString().trim();
			String[] toSplitted = value.toString().split(",");
			for(String to : toSplitted) {
				context.write(new Text(from), new Text(to.trim()));
			}
		}
	}

	/**
	 * Creates a reducer to append all elements with the same key.
	 */
	public static class DictionaryReducer extends
			Reducer<Text, Text, Text, Text> {

		/**
		 * Reduce the keys to a string concatenated.
		 */
		@Override
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			//Used StringBuilder to reduce memory footprint
			StringBuilder sb = new StringBuilder();
			for (Text val : values) {
				sb.append("|");
				sb.append(val);
			}
			//Write the created line to the context.
			context.write(key, new Text(sb.toString()));
		}
	}

}

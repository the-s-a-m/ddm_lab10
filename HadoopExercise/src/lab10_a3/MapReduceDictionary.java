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
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setPartitionerClass(HashPartitioner.class);

		// job.setNumReduceTasks(0);
		job.setReducerClass(DictionaryReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setOutputFormatClass(TextOutputFormat.class);

		// delete output if existing
		FileSystem fs = FileSystem.get(job.getConfiguration());
		fs.delete(new Path(args[1]), true);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class DictionaryMapper extends
			Mapper<Text, Text, Text, Text> {

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

	public static class DictionaryReducer extends
			Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			StringBuilder sb = new StringBuilder();
			for (Text val : values) {
				sb.append("|");
				sb.append(val);
			}
			context.write(key, new Text(sb.toString()));
		}
	}

}

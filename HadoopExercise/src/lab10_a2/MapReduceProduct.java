package lab10_a2;

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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MapReduceProduct extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new MapReduceProduct(), args);
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

		//Set Input format
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(CategoryMapper.class);
		//set output key
		job.setMapOutputKeyClass(Text.class);
		//set output value
		job.setMapOutputValueClass(DoubleWritable.class);

		job.setPartitionerClass(HashPartitioner.class);

		// job.setNumReduceTasks(0);
		job.setReducerClass(CategoryReducer.class);
		//set output key
		job.setOutputKeyClass(Text.class);
		//set output value
		job.setOutputValueClass(DoubleWritable.class);

		job.setOutputFormatClass(TextOutputFormat.class);

		// delete output if existing
		FileSystem fs = FileSystem.get(job.getConfiguration());
		fs.delete(new Path(args[1]), true);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * Creates a mapper with converts LongWritable, Text to Key: Text and DoubleWritable
	 */
	public static class CategoryMapper extends
			Mapper<LongWritable, Text, Text, DoubleWritable> {

		/**
		 * Creates a new mapping for the given text.
		 */
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//Split the content
			String[] parts = value.toString().split("\t");
			if (parts.length >= 5) {
				try {
					//convert to content and catch errors if needed
					context.write(new Text(parts[3].trim()),
							new DoubleWritable(Double.parseDouble(parts[4])));
				} catch (NumberFormatException e) {
				}
			}
		}
	}

	/**
	 * Creates a reducer to sum up all values with the same key.
	 */
	public static class CategoryReducer extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		/**
		 * Reduce the keys to the sum of the values.
		 */
		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {

			double sum = 0;
			for (DoubleWritable val : values) {
				sum += val.get();
			}
			context.write(key, new DoubleWritable(sum));
		}
	}

}

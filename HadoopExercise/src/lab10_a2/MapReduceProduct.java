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

		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(CategoryMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		job.setPartitionerClass(HashPartitioner.class);

		// job.setNumReduceTasks(0);
		job.setReducerClass(CategoryReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setOutputFormatClass(TextOutputFormat.class);

		// delete output if existing
		FileSystem fs = FileSystem.get(job.getConfiguration());
		fs.delete(new Path(args[1]), true);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class CategoryMapper extends
			Mapper<LongWritable, Text, Text, DoubleWritable> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] parts = value.toString().split("\t");
			if (parts.length >= 5) {
				try {
					context.write(new Text(parts[3].trim()),
							new DoubleWritable(Double.parseDouble(parts[4])));
				} catch (NumberFormatException e) {
				}
			}
		}
	}

	public static class CategoryReducer extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {

			int sum = 0;
			for (DoubleWritable val : values) {
				sum += val.get();
			}
			context.write(key, new DoubleWritable(sum));
		}
	}

}

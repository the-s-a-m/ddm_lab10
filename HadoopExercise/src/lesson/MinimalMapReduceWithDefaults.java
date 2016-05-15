package lesson;

import java.io.IOException;
import java.io.StringWriter;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class MinimalMapReduceWithDefaults extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
	    int res = ToolRunner.run(new  MinimalMapReduceWithDefaults(), args);
	    System.exit(res);
	}
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
		    System.err.printf("Usage: %s [generic options] <in> <out>\n", getClass().getSimpleName());
		    ToolRunner.printGenericCommandUsage(System.err);
		    return -1;
			
		}
		Job job = Job.getInstance(getConf(), "minimapredwithdefaults");
		job.setJarByClass(this.getClass());
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setPartitionerClass(HashPartitioner.class);
		
		//job.setNumReduceTasks(0);
		job.setReducerClass(MyReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//delete output if existing
		FileSystem fs = FileSystem.get(job.getConfiguration());
		fs.delete(new Path(args[1]), true);
		
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    return job.waitForCompletion(true) ? 0 : 1;
	  }
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	    private final static LongWritable one = new LongWritable(1);
	    private Text word = new Text();
	    
		  @Override
		  public void map(LongWritable key, Text value, Context context)
		      throws IOException, InterruptedException {
			
			StringTokenizer itr = new StringTokenizer(value.toString());
		      while (itr.hasMoreTokens()) {
		          word.set(itr.nextToken());
		          context.write(word, one);
		        }
		  }
		}

	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		  private LongWritable result = new LongWritable();
			
		  @Override
		  public void reduce(Text key, Iterable<LongWritable> values, Context context)
		      throws IOException, InterruptedException {

		      int sum = 0;
		      for (LongWritable val : values) {
		        sum += val.get();
		      }
		      result.set(sum);
		      context.write(key, result);
		  }
		}	

}


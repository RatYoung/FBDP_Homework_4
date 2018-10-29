import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Combination{
	public static class CombinationMap extends Mapper<LongWritable, Text, Text, IntWritable>{
		private IntWritable one = new IntWritable(1);

		@Override
		public void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException{
			context.write(line, one);
		}
	}

	public static class CombinationReduce extends Reducer<Text, IntWritable, Text, NullWritable>{
		@Override
		public void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException{
			int sum = 0;
			for(IntWritable val : value){
				sum += val.get();
			}
			if(sum > 0)
				context.write(key, NullWritable.get());
			else
				System.out.println("find an exception");
		}
	}

	public static void main(String[] args) throws Exception{
		System.out.println("Job started");
		Configuration conf = new Configuration();
		Job combinationJob = new Job(conf, "combinationJob");
		//combinationJob.setJobName("combinationJob");
		combinationJob.setJarByClass(Combination.class);
		combinationJob.setMapperClass(CombinationMap.class);
		combinationJob.setReducerClass(CombinationReduce.class);
		combinationJob.setMapOutputKeyClass(Text.class);
		combinationJob.setMapOutputValueClass(IntWritable.class);
		combinationJob.setOutputValueClass(NullWritable.class);
        combinationJob.setOutputKeyClass(Text.class);
		//combinationJob.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(combinationJob, new Path(args[0]), new Path(args[1]));
        FileOutputFormat.setOutputPath(combinationJob, new Path(args[2]));
        combinationJob.waitForCompletion(true);
        System.out.println("Finished!");
	}
}
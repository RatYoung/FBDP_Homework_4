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

public class Difference{
	public static class DifferenceMap extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		public void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException{
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
      		String fileName = fileSplit.getPath().getName();

      		if (fileName.contains("1")){
      			Text setA = new Text();
      			setA.set("setA");
      			context.write(line, setA);
      		}
      		else if (fileName.contains("2")){
      			Text setB = new Text();
      			setB.set("setB");
      			context.write(line, setB);
      		}
		}
	}

	public static class DifferenceReduce extends Reducer<Text, Text, Text, NullWritable>{
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			for (Text val : values){
				if (!val.toString().equals("setA"))
					return;
			}
			context.write(key, NullWritable.get());
		}
	}

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();

		Job differenceJob = new Job(conf, "differenceJob");
		differenceJob.setJarByClass(Difference.class);
		differenceJob.setMapperClass(DifferenceMap.class);
		differenceJob.setReducerClass(DifferenceReduce.class);
		differenceJob.setMapOutputKeyClass(Text.class);
		differenceJob.setMapOutputValueClass(Text.class);
		differenceJob.setOutputKeyClass(Text.class);
		differenceJob.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(differenceJob, new Path(args[0]), new Path(args[1]));
        FileOutputFormat.setOutputPath(differenceJob, new Path(args[2]));

        differenceJob.waitForCompletion(true);

        System.out.println("Finished!");
	}
}
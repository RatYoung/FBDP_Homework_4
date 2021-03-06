import java.io.IOException;
import java.util.ArrayList;

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

public class NatureJoin{
	
	public static class NatureJoinMap extends Mapper<LongWritable, Text, IntWritable, Text>{
		@Override
		public void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException{
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
      		String fileName = fileSplit.getPath().getName();

			if (fileName.contains("a")){
				String[] lineStr = line.toString().split(",");
				IntWritable id = new IntWritable(Integer.parseInt(lineStr[0]));

				Text attributes = new Text();

				attributes.set("setA"+","+lineStr[1]+","+lineStr[2]+","+lineStr[3]);
				context.write(id, attributes);
			}
			else if (fileName.contains("b")){
				String[] lineStr = line.toString().split(",");
				IntWritable id = new IntWritable(Integer.parseInt(lineStr[0]));

				Text attributes = new Text();

				attributes.set("setB"+","+lineStr[1]+","+lineStr[2]);
				context.write(id, attributes);
			}
		}	
	}

	public static class NatureJoinReduce extends Reducer<IntWritable, Text, NullWritable, Text>{
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			int count = 0;
			String setAstr = new String();
			String setBstr = new String();
			ArrayList<Text>attributes = new ArrayList<Text>();

			for(Text val : values){
				count += 1;
				System.out.println(val);
				Text each = new Text();
				each.set(val.toString());
				attributes.add(each);
			}

			if (count == 1)
				return;
			else if (count == 2){
				//System.out.println(attributes.get(0));
				//System.out.println(attributes.get(1));
				for(int i = 0; i < attributes.size(); i++){
					if (attributes.get(i).toString().split(",")[0].equals("setA")){
						setAstr = attributes.get(i).toString();
					}
					else if(attributes.get(i).toString().split(",")[0].equals("setB")){
						setBstr = attributes.get(i).toString();
						//System.out.println("setB");
					}
				}
				//String[] array_setBstr = setBstr.split(",");
				Text result = new Text();
				result.set(key.toString()+","+setAstr.split(",")[1]+","+setAstr.split(",")[2]+","+setAstr.split(",")[3]+","+setBstr.split(",")[1]+","+setBstr.split(",")[2]);
				//result.set(key.toString()+","+setAstr+","+setBstr);
				context.write(NullWritable.get(), result);
			}
		}
	}


	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();

		Job natureJoinJob = new Job(conf, "natureJoinJob");
		natureJoinJob.setJarByClass(NatureJoin.class);
		natureJoinJob.setMapperClass(NatureJoinMap.class);
		natureJoinJob.setReducerClass(NatureJoinReduce.class);
		natureJoinJob.setMapOutputKeyClass(IntWritable.class);
		natureJoinJob.setMapOutputValueClass(Text.class);
		natureJoinJob.setOutputKeyClass(NullWritable.class);
		natureJoinJob.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(natureJoinJob, new Path(args[0]), new Path(args[1]));
        FileOutputFormat.setOutputPath(natureJoinJob, new Path(args[2]));

        natureJoinJob.waitForCompletion(true);

        System.out.println("Finished!");
	}
}
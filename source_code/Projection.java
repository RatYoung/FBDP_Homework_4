import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Projection {
	public static class RelationA{
		private int id;
		private String name;
		private int age;
		private int weight;

		public RelationA(String line){
			String[] columns = line.split(",");
			id = Integer.parseInt(columns[0]);
			name = columns[1];
			age = Integer.parseInt(columns[2]);
			weight = Integer.parseInt(columns[3]);
		}

		public int isCondition(String col){
			if(col.equals("id"))
				return 0;
			if(col.equals("name"))
				return 1;
			if(col.equals("age"))
				return 2;
			if(col.equals("weight"))
				return 3;
			return -1;
		}
	}

	public static class ProjectionMap extends Mapper<LongWritable, Text, Text, NullWritable>{
		private String col;

		@Override
		public void setup(Context context){
			col = context.getConfiguration().get("col");
		}

		@Override
		public void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException{
			RelationA record = new RelationA(line.toString());
			if(record.isCondition(col) >= 0){
				Text text = new Text();
				text.set(line.toString().split(",")[record.isCondition(col)]);
				context.write(text, NullWritable.get());
            }		
		}
	}

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		conf.set("col", args[2]);

		Job projectionJob = new Job(conf, "projectionJob");
		projectionJob.setJarByClass(Projection.class);
		projectionJob.setMapperClass(ProjectionMap.class);
		projectionJob.setMapOutputKeyClass(Text.class);
		projectionJob.setMapOutputValueClass(NullWritable.class);
		projectionJob.setNumReduceTasks(0);
		projectionJob.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(projectionJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(projectionJob, new Path(args[1]));
        projectionJob.waitForCompletion(true);
        System.out.println("Finished!");
	}
}
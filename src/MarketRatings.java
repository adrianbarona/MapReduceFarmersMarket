package MarketRatingsPkg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MarketRatings {
	
	//Main class calls all other MapReduce classes

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "ratings");
		job.setJarByClass(MarketRatings.class);
		job.setMapperClass(MapClass.class);		
		job.setCombinerClass(Reduce.class);		
		job.setPartitionerClass(Partition.class);
		job.setNumReduceTasks(6);		
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);		
		job.getConfiguration().set("mapred.job.queue.name", ""); //configuration name removed		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

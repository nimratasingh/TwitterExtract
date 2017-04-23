package pkg;

import java.io.IOException;
import java.net.URISyntaxException;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.*;

public class MainParser {
	public static final String OUTPUT_PATH1 = "OutputJob1";

	public static void main(String[] args) throws IOException, ClassNotFoundException,InterruptedException,URISyntaxException{
		// TODO Auto-generated method stub

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(MainParser.class);
		job.setMapperClass(TwitterExtractMapper.class);
		job.setReducerClass(TwitterExtractReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
//		FileInputFormat.setInputDirRecursive(job, true);
		FileInputFormat.setInputPaths(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH1));
		if(job.waitForCompletion(true)){
			System.out.println("Job1 COMPLETION!!");
		}

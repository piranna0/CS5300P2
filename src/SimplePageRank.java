import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class SimplePageRank 
{
	public static enum MY_COUNTERS {
		BLAH
	};
	
	// input (phase 1): <line#, line>
	// input (phase >1): <<outlink, rank>, list of inlinks>
	// output (for each inlink): <inlink, <outlink, rank>>
	public static class Map extends MapReduceBase implements Mapper<SimpleKey, SimpleValue, Text, Tuple<Text, LongWritable>> 
	{
		private Text outlink = new Text();
		private Text inlink = new Text();

		//public void map(LongWritable key, Text value, OutputCollector<Text, Tuple<Text, LongWritable>> output, Reporter reporter) throws IOException 
		public void map(SimpleKey key, SimpleValue value, OutputCollector<Text, Tuple<Text, LongWritable>> output, Reporter reporter) throws IOException 
		{
			// read from simplekey and simplevalue
			
			// process: reverse outlinks
			
			output.collect();
			
			// increment iteration counter
			reporter.getCounter(MY_COUNTERS.BLAH).increment(1);
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, SimpleKey, SimpleValue> 
	{
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<SimpleKey, SimpleValue> output, Reporter reporter) throws IOException 
		{
			int sum = 0;
			while (values.hasNext()) 
			{
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception 
	{
		JobConf conf = new JobConf(WordCount.class);
		conf.setJobName("wordcount");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		// TODO: input and output paths should be s3
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		// run the job
		RunningJob rj = JobClient.runJob(conf);
		
		Counters c = rj.getCounters();
		long counter = c.getCounter(MY_COUNTERS.BLAH);

		// write counter values to an output file
		try 
		{
			PrintWriter writer = new PrintWriter("testCounter.txt");
			writer.write("counter: " + Long.toString(counter));
			writer.close();
		} 
		catch (FileNotFoundException e)
		{
			e.printStackTrace();
		}
	}
}
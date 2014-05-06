import java.io.IOException;
import java.io.PrintWriter;
import java.io.FileNotFoundException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class WordCount2
{
	public static enum MY_COUNTERS {
		BLAH
	};

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> 
	{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException 
		{
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) 
			{
				word.set(tokenizer.nextToken());
				output.collect(word, one);
			}
			
			// increment iteration counter
			reporter.getCounter(MY_COUNTERS.BLAH).increment(1);
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> 
	{
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException 
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
		JobConf conf = new JobConf(WordCount2.class);
		conf.setJobName("wordcount");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		//FileInputFormat.setInputPaths(conf, new Path("s3://edu-cornell-cs-cs5300s14-gws55.s3.amazonaws.com/wordcount/input/"));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		RunningJob rj = JobClient.runJob(conf);
		Counters c = rj.getCounters();
		long counter = c.getCounter(MY_COUNTERS.BLAH);

		try {
			PrintWriter writer = new PrintWriter("testCounter.txt");
			writer.write(Long.toString(counter));
			writer.close();
		} catch (FileNotFoundException e)
		{
			e.printStackTrace();
		}
	
	}
}
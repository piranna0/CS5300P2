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
	
	private static int[] null_array = new int[0];
	
	public static class Tuple<X, Y> 
	{ 
		public final X x; 
		public final Y y; 	
		public Tuple(X x, Y y) 
		{ 
			this.x = x; 
			this.y = y; 
		} 
	} 
	
	// parses the raw Text input values
	public static Tuple<Double, int[]> parseValue(Text value)
	{
		String[] parts = value.toString().split("_");
		Double pagerank = Double.parseDouble(parts[0]);
		int[] outlinks = new int[parts.length - 1];
		for (int i = 1; i < parts.length; i++)
		{
			outlinks[i-1] = Integer.parseInt(parts[i]);
		}
		
		return new Tuple<Double, int[]>(pagerank, outlinks);
	}
	
	// constructs the raw Text value from the PR and outlink array
	public static Text constructValue(double pr, int[] outlinks)
	{
		String s = Double.toString(pr);
		for (int i = 0; i < outlinks.length; i++)
		{
			s += "_" + outlinks[i];
		}
		
		return new Text(s);
	}
	
	// input: <<outlink, rank>, list of inlinks>
	// output (for each inlink): <inlink, <outlink, rank>>
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> 
	{		
		public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException 
		{
			// extract from value
			Tuple<Double, int[]> parsed_value = parseValue(value);
			double pagerank = parsed_value.x;
			int[] outlinks = parsed_value.y;
			double N = outlinks.length;
			
			// reverse links and emit 
			for (int i = 0; i < N; i++)
			{
				LongWritable writable_outlink = new LongWritable(outlinks[i]);
				output.collect(writable_outlink, constructValue(pagerank / N, null_array));
			}
			
			// emit the set of outlinks
			output.collect(key, value);
			
			// increment iteration counter
			reporter.getCounter(MY_COUNTERS.BLAH).increment(1);
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, Text> 
	{
		public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException 
		{
			Text v;
			double pagerank;
			int[] links;
			int[] outlinks = new int[0];
			double N = 0;
			long ranksum = 0;

			// extract from values
			while (values.hasNext())
			{
				v = values.next();
				Tuple<Double, int[]> parsed_value = parseValue(v);
				pagerank = parsed_value.x;
				links = parsed_value.y;
				N = links.length;
				
				// if v is an inlink, accumulate its pagerank
				if (N == 0)
				{
					ranksum += pagerank / N;
				}
				// otherwise, it's the list of outlinks
				else
				{
					outlinks = links;
				}
			}

			output.collect(key, constructValue(ranksum, outlinks));
		}
	}

	public static void main(String[] args) throws Exception 
	{
		JobConf conf = new JobConf(SimplePageRank.class);
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
		
		// get counters
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
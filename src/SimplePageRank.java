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
		RESIDUAL
	};

	//static int N = 685230; //Number of nodes
	static int N = 5; //Number of nodes
	static double d = .85; //Damping factor
	private static int NUM_PASSES = 5;	// number of passes of mapreduce
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


	//Input/Output Text Format: 
	//u;PR(u)_v_w_x...
	// input: <<outlink, rank>, list of inlinks>
	// output (for each inlink): <inlink, <outlink, rank>>
	public static class Map extends MapReduceBase implements Mapper<Text, Text, Text, Text> 
	{		
		public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
		{
			// extract from value
			Tuple<Double, int[]> parsed_value = parseValue(value);
			double pagerank = parsed_value.x;
			int[] outlinks = parsed_value.y;
			double N = outlinks.length;

			// reverse links and emit 
			for (int i = 0; i < N; i++)
			{
				Text text_outlink = new Text(Integer.toString(outlinks[i]));
				output.collect(text_outlink, constructValue(pagerank / N, null_array));
			}

			// emit the set of outlinks
			output.collect(key, value);
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> 
	{
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
		{
			Text v;
			double pagerank;
			double pagerankPrev = 0;
			int[] links;
			int[] outlinks = new int[0];
			int numInlinks = 0;
			double pageRankSum = 0;

			// extract from values
			while (values.hasNext())
			{
				v = values.next();
				Tuple<Double, int[]> parsed_value = parseValue(v);
				pagerank = parsed_value.x;
				links = parsed_value.y;
				numInlinks = links.length;

				if (numInlinks == 0) // if v is an inlink, accumulate its pagerank
				{
					pageRankSum += pagerank;
				}
				else // otherwise, it's the list of outlinks
				{
					pagerankPrev = pagerank;
					outlinks = links;
				}
			}
			double pageRankTotal = ((1-d) / N) + (pageRankSum * d);

			output.collect(key, constructValue(pageRankTotal, outlinks));

			// calculate residual and increment counter
			double residual = (Math.abs(pagerankPrev - pageRankTotal) / pageRankTotal) * 10000;
			reporter.getCounter(MY_COUNTERS.RESIDUAL).increment((long)residual);
		}
	}

	public static void main(String[] args) throws Exception 
	{
		JobConf conf = new JobConf(SimplePageRank.class);
		conf.setJobName("wordcount");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(KeyValueTextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ";");
		conf.set("mapreduce.output.textoutputformat.separator", ";");

		// run the job
		RunningJob rj = null;
		Path prevPath = null;
		long[] avg_residuals = new long[NUM_PASSES];	// stores the residuals for each pass (index = pass number)
		for (int i = 0; i < NUM_PASSES; i++)
		{
			// TODO: input and output paths should be s3
			// input path
			if (i == 0)
			{
				FileInputFormat.setInputPaths(conf, new Path(args[0]));
			}
			else
			{
				FileInputFormat.setInputPaths(conf, prevPath);
			}
			
			// output path
			prevPath = new Path("./simplepagerank_output" + Integer.toString(i));
			FileOutputFormat.setOutputPath(conf, prevPath);
			
			// run job
			rj = JobClient.runJob(conf);
			
			// read from counters
			Counters c = rj.getCounters();
			long residual_counter = c.getCounter(MY_COUNTERS.RESIDUAL) / (long)10000;
			avg_residuals[i] = residual_counter / N;
		}
		
		// write avg_residual values to an output file
		try 
		{
			PrintWriter writer = new PrintWriter("simplepagerank_output.txt");
			for (int i = 0; i < NUM_PASSES; i++)
			{
				writer.write("Iteration " + Integer.toString(i) + " avg residual " + Long.toString(avg_residuals[i]) + '\n');
			}
			writer.close();
		} 
		catch (FileNotFoundException e)
		{
			e.printStackTrace();
		}
	}
}
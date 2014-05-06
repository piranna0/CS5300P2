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
	
	// input: <<outlink, rank>, list of inlinks>
	// output (for each inlink): <inlink, <outlink, rank>>
	public static class Map extends MapReduceBase implements Mapper<Integer, SimpleValue, Integer, SimpleValue> 
	{
		private Text outlink = new Text();
		private Text inlink = new Text();

		public void map(Integer key, SimpleValue value, OutputCollector<Integer, SimpleValue> output, Reporter reporter) throws IOException 
		{
			// extract from value
			double pagerank = value.pagerank;
			int[] outlinks = value.nodes;
			double N = outlinks.length;
			
			// reverse links and emit 
			for (int i = 0; i < N; i++)
			{
				output.collect(outlinks[i], new SimpleValue(pagerank / N, null_array));
			}
			
			// emit the set of outlinks
			output.collect(key, value);
			
			// increment iteration counter
			reporter.getCounter(MY_COUNTERS.BLAH).increment(1);
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Integer, SimpleValue, Integer, SimpleValue> 
	{
		public void reduce(Integer key, Iterator<SimpleValue> values, OutputCollector<Integer, SimpleValue> output, Reporter reporter) throws IOException 
		{
			SimpleValue v;
			double pagerank;
			int[] links;
			int[] outlinks = new int[0];
			double N = 0;
			long ranksum = 0;

//			// count how many values there are
//			while (values.hasNext())
//			{
//				values.next();
//				N++;
//			}
			
			// extract from values
			while (values.hasNext())
			{
				v = values.next();
				pagerank = v.pagerank;
				links = v.nodes;
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
			
			output.collect(key, new SimpleValue(ranksum, outlinks));
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
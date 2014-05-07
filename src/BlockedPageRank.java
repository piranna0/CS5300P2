import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class BlockedPageRank 
{	
	public static enum MY_COUNTERS {
		RESIDUAL
	};

	static int N = 685230; //Number of nodes
	static double d = .85; //Damping factor
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
	
	public static class Fiveple<V,W,X,Y,Z> 
	{ 
		public final V v;
		public final W w; 
		public final X x; 
		public final Y y; 
		public final Z z;
		public Fiveple(V v, W w, X x, Y y, Z z) 
		{  
			this.v = v;
			this.w = w;
			this.x = x;
			this.y = y;
			this.z = z;
		} 
	}

	// parses the raw Text input values
	public static Fiveple<Integer, Integer, Double, int[], int[]> parseValue(Text value)
	{
		String[] parts = value.toString().split("_");
		Integer node = Integer.parseInt(parts[0]);
		Integer fromBlock = Integer.parseInt(parts[1]);
		Double pagerank = Double.parseDouble(parts[2]);
		int[] outlink_blocks = new int[parts.length - 3];
		int[] outlink_nodes = new int[parts.length - 3];
		
		// if there are outlinks from this node, pass them through the output arrays
		if (!parts[3].equals("-1"))
		{
			for (int i = 3; i < parts.length; i++)
			{
				String[] inner_parts = parts[i].split("~");
				outlink_blocks[i-3] = Integer.parseInt(inner_parts[0]);
				outlink_nodes[i-3] = Integer.parseInt(inner_parts[1]);
			}
		}
		// otherwise, pass -1's in the output arrays
		else
		{
			// these two arrays should be length 1
			outlink_blocks[0] = -1;
			outlink_nodes[0] = -1;
		}

		return new Fiveple<Integer, Integer, Double, int[], int[]>(node, fromBlock, pagerank, outlink_blocks, outlink_nodes);
	}

	// constructs the raw Text value
	public static Text constructValue(Text node, int block, double pr, int[] outlink_blocks, int[] outlink_nodes)
	{
		String s = node + "_" + Integer.toString(block) + "_" + Double.toString(pr);
		for (int i = 0; i < outlink_blocks.length; i++)
		{
			s += "_" + outlink_blocks[i] + "~" + outlink_nodes[i];
		}

		return new Text(s);
	}

	// input: block(u); u, -1, PR(u), {block(v), v | u->v}
	// output: block(u); u, -1, PR(u), {block(v), v | u->v}
	//         block(v); v, b(u), PR(u)/N, -1
	public static class Map extends MapReduceBase implements Mapper<Text, Text, Text, Text> 
	{		
		public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
		{
			PrintWriter writer = new PrintWriter("MapEmit" + key.toString() + ".txt");
			// extract values
			int block = Integer.parseInt(key.toString());
			Fiveple<Integer, Integer, Double, int[], int[]> parsed_value = parseValue(value);
			int node = parsed_value.v;
			int from_block = parsed_value.w;
			double pagerank = parsed_value.x;
			int[] outlink_blocks = parsed_value.y;
			int[] outlink_nodes = parsed_value.z;
			double N = outlink_blocks.length;

			if(outlink_blocks[0] != -1)
			{
				// reverse links and emit 
				for (int i = 0; i < N; i++)
				{
					Text text_outlink_block = new Text(Integer.toString(outlink_blocks[i]));
					Text text_outlink_node = new Text(Integer.toString(outlink_nodes[i]));
					output.collect(text_outlink_block, constructValue(text_outlink_node, block, pagerank / N, null_array, null_array));
				}
				writer.close();
			}

			// emit the set of outlinks
			output.collect(key, value);
		}
	}

	// input: block(u); u, -1, PR(u), {block(v), v | u->v}
	//        block(v); v, b(u), PR(u)/N, -1
	// output: block(u); u, -1, PR(u), {block(v), v | u->v}
	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> 
	{
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
		{
			Text v;
			int block = Integer.parseInt(key.toString());
			int node = -1;
			int from_block;
			double pagerank;
			double pagerankPrev = 0;
			int[] outlink_blocks = null;
			int[] outlink_nodes = null;
			int[] new_outlink_blocks = new int[0];
			int[] new_outlink_nodes = new int[0];
			int numInlinks = 0;
			double pageRankSum = 0;
			
			// extract from values
			while (values.hasNext())
			{
				v = values.next();
				Fiveple<Integer, Integer, Double, int[], int[]> parsed_value = parseValue(v);
				node = parsed_value.v;
				from_block = parsed_value.w;
				pagerank = parsed_value.x;
				outlink_blocks = parsed_value.y;
				outlink_nodes = parsed_value.z;
				numInlinks = outlink_blocks.length;
				
				if (numInlinks == 0) // if v is an inlink
				{
					pageRankSum += pagerank;
				}
				else // otherwise, it's the list of outlinks
				{
					pagerankPrev = pagerank;
					new_outlink_blocks = outlink_blocks;
					new_outlink_nodes = outlink_nodes;
				}
			}
			double pageRankTotal = ((1-d) / N) + (pageRankSum * d);

			output.collect(key, constructValue(new Text(Integer.toString(node)), -1, pageRankTotal, outlink_blocks, outlink_nodes));

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
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(KeyValueTextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ";");
		conf.set("mapreduce.output.textoutputformat.separator", ";");

		// run the job
		RunningJob rj = null;
		Path prevPath = null;
		double[] avg_residuals = new double[NUM_PASSES];	// stores the residuals for each pass (index = pass number)
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
			double residual_counter = c.getCounter(MY_COUNTERS.RESIDUAL) / 10000.;
			avg_residuals[i] = residual_counter / N;
		}
		
		// write avg_residual values to an output file
		try 
		{
			PrintWriter writer = new PrintWriter("simplepagerank_output.txt");
			for (int i = 0; i < NUM_PASSES; i++)
			{
				writer.write("Iteration " + Integer.toString(i) + " avg residual " + Double.toString(avg_residuals[i]) + '\n');
			}
			writer.close();
		} 
		catch (FileNotFoundException e)
		{
			e.printStackTrace();
		}
	}
}
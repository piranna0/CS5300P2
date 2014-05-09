import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

public class BlockedPageRank 
{	
	public static enum MY_COUNTERS {
		RESIDUAL, ITERATIONS
	};

	static int N = 685230; //Number of nodes
	static double d = .85; //Damping factor
	static double THRESHOLD = .001; //BlockedPageRank Reduce stopping criterion
	static final int NUM_BLOCKS = 68; 
	private static int[] null_array = new int[0];

	//final pageranks for the largest nodes in each block
	public static TreeMap<Integer, Double> pagerank_final = new TreeMap<Integer,Double>();

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

	public static class Fiveple
	{ 
		public final int v;
		public final int w; 
		public double x;
		public final int[] y; 
		public final int[] z;
		public Fiveple(int v, int w, double x, int[] y, int[] z) 
		{  
			this.v = v;
			this.w = w;
			this.x = x;
			this.y = y;
			this.z = z;
		} 
		@Override
		public String toString() {
			return v+" "+w+" "+x+" "+y+" "+z;
		}
	}

	// parses the raw Text input values
	public static Fiveple parseValue(Text value)
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

		return new Fiveple(node, fromBlock, pagerank, outlink_blocks, outlink_nodes);
	}

	// constructs the raw Text value
	public static Text constructValue(Text node, Integer block, Double pr, int[] outlink_blocks, int[] outlink_nodes)
	{
		String s = node + "_" + block.toString() + "_" + pr.toString();
		if (outlink_blocks.length==0) {
			return new Text(s+"_-1");
		}

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
			//			PrintWriter writer = new PrintWriter("MapEmit" + key.toString() + ".txt");
			// extract values
			int block = Integer.parseInt(key.toString());
			Fiveple parsed_value = parseValue(value);
			//			int node = parsed_value.v;
			//			int from_block = parsed_value.w;
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
				//				writer.close();
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
			int block = Integer.parseInt(key.toString());
			// nodes with lists of outlinks used to emit
			ArrayList<Fiveple> out_nodes = new ArrayList<Fiveple>();
			HashMap<Integer, Double> constantPR = new HashMap<Integer,Double>(); // from outside the block
			HashMap<Integer, Double> changingPR = new HashMap<Integer,Double>(); //from inside the block
			HashMap<Integer, Double> initialPR = new HashMap<Integer,Double>(); //for final residual

			while (values.hasNext()) {
				Text v = values.next();
				Fiveple parsed_value = parseValue(v);
				int[] outlink_blocks = parsed_value.y;
				int from_block = parsed_value.w;
				double pagerank = parsed_value.x;
				int node = parsed_value.v;

				if (from_block==-1) { // v is an inlink
					out_nodes.add(parsed_value);
					initialPR.put(node, pagerank);
				} else {
					double sum = 0;
					if (from_block==block) { //from inside the block: changing
						if (changingPR.get(node)!=null) {
							sum = changingPR.get(node);
						}
						changingPR.put(node,pagerank+sum);
					} else { // from outside the block: constant
						if (constantPR.get(node)!=null) {
							sum = constantPR.get(node);
						}
						constantPR.put(node, pagerank+sum);
					}
				}

			}

			int num_nodes_in_block = out_nodes.size();

			double total_residual = Double.MAX_VALUE;

			int iterations = 0;
			while (total_residual/num_nodes_in_block > THRESHOLD) {
				iterations += 1;
				total_residual = 0;
				// the next changingPR
				HashMap<Integer, Double> changingPRNext = new HashMap<Integer,Double>(num_nodes_in_block);

				for (Fiveple node_fiveple : out_nodes) {
					// for all nodes in block:
					int node = node_fiveple.v;
					double pagerankPrev = node_fiveple.x;
					int[] outlink_blocks = node_fiveple.y;
					int[] outlink_nodes = node_fiveple.z;

					//calculate pagerank
					double changingContribution = 0;
					double constantContribution = 0;
					if (constantPR.get(node)!= null) constantContribution = constantPR.get(node);
					if (changingPR.get(node)!= null) changingContribution = changingPR.get(node);
					double pageRankTotal = ((1-d) / N) + ((constantContribution+changingContribution) * d);

					//calculate residuals
					double residual = (Math.abs(pagerankPrev - pageRankTotal) / pageRankTotal);
					total_residual += residual;

					//update pageranks
					node_fiveple.x = pageRankTotal;
					for (int i=0; i<outlink_nodes.length; i++) {
						if (outlink_blocks[i]==block) {
							double sum = 0;
							if (changingPRNext.get(outlink_nodes[i])!=null) {
								sum = changingPRNext.get(outlink_nodes[i]);
							}
							changingPRNext.put(outlink_nodes[i], sum + pageRankTotal/outlink_blocks.length);
						}
					}
				}
				changingPR = changingPRNext;
				//				PrintWriter writer = new PrintWriter(new FileWriter("./output/Intermediate" + key.toString() + ".txt", true));
				//				writer.write("block: " + block + " resid: " + total_residual/num_nodes_in_block + "\n");
				//				writer.write("total_resid: " + total_residual+ "\n");
				//				writer.close();
			}

			total_residual = 0;
			int maxnode = 0;
			double maxnode_pagerank = 0;
			for (Fiveple node_fiveple : out_nodes) {
				// for all nodes in block, emit
				Integer node = node_fiveple.v;
				double pagerank = node_fiveple.x;
				double residual = (Math.abs(initialPR.get(node) - pagerank) / pagerank);
				total_residual += residual;
				int[] outlink_blocks = node_fiveple.y;
				int[] outlink_nodes = node_fiveple.z;
				output.collect(key, constructValue(new Text(node.toString()), -1, pagerank, outlink_blocks, outlink_nodes));

				if (maxnode<node) {
					maxnode = node;
					maxnode_pagerank = pagerank;
				}
			}
			pagerank_final.put(maxnode, maxnode_pagerank);

			reporter.getCounter(MY_COUNTERS.RESIDUAL).increment((long)total_residual*10000);
			reporter.getCounter(MY_COUNTERS.ITERATIONS).increment((long)iterations);

			//			PrintWriter writer = new PrintWriter(new FileWriter("./output/Reduce" + key.toString() + ".txt", true));
			//			writer.write("block: " + block + " resid: " + total_residual/num_nodes_in_block + "\n");
			//			writer.close();

		}
	}

	public static void writeS3(String bucket, String key, String message){
		AmazonS3 s3 = new AmazonS3Client(new BasicAWSCredentials("AKIAIQJUZCRBWBDPMNTQ","1icMGeE13StjnE7pSLpDHd0d6s8mOa+PKZpGXwHo"));
		//		AmazonS3 s3 = new AmazonS3Client(new ClasspathPropertiesFileCredentialsProvider());
		//		GetObjectRequest getObject = new GetObjectRequest(bucket, key);
		//		S3Object object = s3.getObject(getObject);
		byte[] bytes = message.getBytes();
		InputStream is = new ByteArrayInputStream(bytes);
		ObjectMetadata meta = new ObjectMetadata();
		meta.setContentLength(bytes.length);
		PutObjectRequest putObject = new PutObjectRequest(bucket, key, is, meta);
		s3.putObject(putObject);

	}

	public static String getMaxPagerankBlock(Path prevPath) throws IOException{
		TreeMap<Integer, PagerankTuple> tm = new TreeMap<Integer, PagerankTuple>();
		String line;
		PagerankTuple tuple;

		int[] blocks = {10328, 20373, 30629, 40645, 50462, 60841, 70591
				, 80118
				, 90497
				,100501
				,110567
				,120945
				,130999
				,140574
				,150953
				,161332
				,171154
				,181514
				,191625
				,202004
				,212383
				,222762
				,232593
				,242878
				,252938
				,263149
				,273210
				,283473
				,293255
				,303043
				,313370
				,323522
				,333883
				,343663
				,353645
				,363929
				,374236
				,384554
				,394929
				,404712
				,414617
				,424747
				,434707
				,444489
				,454285
				,464398
				,474196
				,484050
				,493968
				,503752
				,514131
				,524510
				,534709
				,545088
				,555467
				,565846
				,576225
				,586604
				,596585
				,606367
				,616148
				,626448
				,636240
				,646022
				,655804
				,665666
				,675448
				,685230};
		FileSystem fs = FileSystem.get(new Configuration());
		FileStatus[] status = fs.listStatus(prevPath);
		for(int i = 0; i < status.length; i++){
			String pathName = status[i].getPath().getName();
			if(pathName.contains("part") && !pathName.contains("crc")){
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));

				while((line = br.readLine()) != null){
					String[] keyValue= line.split(";");
					int block = Integer.parseInt(keyValue[0]);

					String[] parts = keyValue[1].toString().split("_");
					Integer node = Integer.parseInt(parts[0]);
					Double pagerank = Double.parseDouble(parts[2]);


					for(int j = 0; j < blocks.length; j++){
						if(node == blocks[j]-1){
							tm.put(block, new PagerankTuple(node, pagerank));
						}
					}
//					if((tuple = tm.get(block)) != null){
//						if(tuple.pagerank < pagerank){
//							tm.put(block, new PagerankTuple(node, pagerank));
//						}
//					}
//					else{
//						tm.put(block, new PagerankTuple(node, pagerank));
//					}
				}
			}
		}
		StringBuffer buf = new StringBuffer();
		for(int k : tm.keySet()){
			buf.append("block: " + k + " " + tm.get(k) + "\n");
		}
		return buf.toString();
	}

	public static class PagerankTuple{
		int node; 
		double pagerank;
		public PagerankTuple(Integer node, double pagerank){
			this.node = node;
			this.pagerank = pagerank;
		}

		public String toString(){
			return "node: " + node + " pagerank: " + pagerank;
		}
	}

	public static void main(String[] args) throws Exception 
	{
		String bucket = "";
		String inputKey = "";
		String outputDirectory = "";
		String finalOutputKey = "";
		if(args.length < 4){
			bucket = "edu-cornell-cs-cs5300s14-jkf49";
			inputKey = "/blockinput/";
			outputDirectory = "./blockedpagerank_output";
			finalOutputKey = "blockedResiduals.txt";
		}
		else{
			bucket = args[0];
			inputKey = args[1];
			outputDirectory = args[2];
			finalOutputKey = args[3];
		}

		JobConf conf = new JobConf(BlockedPageRank.class);
		conf.setJobName("blockedpagerank");

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
		ArrayList<Double> avg_residuals = new ArrayList<Double>();
		ArrayList<Double> iterations_arr = new ArrayList<Double>(); 
		double last_residual = Double.MAX_VALUE;
		int i = 0;
		while (last_residual > THRESHOLD)
		{
			// TODO: input and output paths should be s3
			// input path
			if (i == 0)
			{
				FileInputFormat.setInputPaths(conf, new Path("s3n://" + bucket + inputKey));
				//				FileInputFormat.setInputPaths(conf, new Path("s3n://" + bucketName + "/blockinput/"));
				//				FileInputFormat.setInputPaths(conf, new Path(args[0]));
			}
			else
			{
				FileInputFormat.setInputPaths(conf, prevPath);
			}

			// output path
			//			FileSystem fs = FileSystem.get(new Configuration());
			prevPath = new Path(outputDirectory + Integer.toString(i));
			//			fs.delete(prevPath, true);
			FileOutputFormat.setOutputPath(conf, prevPath);

			// run job
			rj = JobClient.runJob(conf);

			// read from counters
			Counters c = rj.getCounters();
			double residual_counter = c.getCounter(MY_COUNTERS.RESIDUAL) / 10000.;
			double iterations = c.getCounter(MY_COUNTERS.ITERATIONS) / NUM_BLOCKS;
			iterations_arr.add(iterations);
			avg_residuals.add(residual_counter / N);
			last_residual = residual_counter/N;
			i+=1;
		}

		StringBuffer buf = new StringBuffer();
		i=0;

		for (int j=0; j<avg_residuals.size(); j++)
		{
			String s1 = Double.toString(avg_residuals.get(j));
			String s2 = Double.toString(iterations_arr.get(j));
			buf.append("Iteration " + Integer.toString(j) + " avg residual " + s1 + " , avg iterations " + s2+ "\n");
		}
		buf.append(getMaxPagerankBlock(prevPath));

		writeS3(bucket, finalOutputKey, buf.toString());

		// write avg_residual values to an output file
		//		try 
		//		{
		//			PrintWriter writer = new PrintWriter("blockedpagerank_output.txt");
		//			for (int j=0; j<avg_residuals.size(); j++)
		//			{
		//				String s1 = Double.toString(avg_residuals.get(j));
		//				String s2 = Double.toString(iterations_arr.get(j));
		//				writer.write("Iteration " + Integer.toString(j) + " avg residual " + s1 + " , avg iterations " + s2+ "\n");
		//			}
		//			writer.write(getMaxPagerankBlock(prevPath));
		//			writer.close();
		//		} 
		//		catch (FileNotFoundException e)
		//		{
		//			e.printStackTrace();
		//		}
	}
}
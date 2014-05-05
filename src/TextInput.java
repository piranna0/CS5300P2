import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.mapred.*;

public class TextInput implements InputFormat<Object, Object>{

	@Override
	public RecordReader<Object, Object> getRecordReader(InputSplit arg0,
			JobConf arg1, Reporter arg2) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InputSplit[] getSplits(JobConf arg0, int arg1) throws IOException {
		Path[] paths = FileInputFormat.getInputPaths(arg0);
		Path edges = new Path(paths[0], "edges.txt");
		Path nodes = new Path(paths[0], "nodes.txt");
		Path blocks = new Path(paths[0], "blocks.txt");
		BufferedReader edge_reader = new BufferedReader(new FileReader(new File(edges.toString())));
		BufferedReader node_reader = new BufferedReader(new FileReader(new File(nodes.toString())));
		BufferedReader blocks_reader = new BufferedReader(new FileReader(new File(blocks.toString())));
		return null;
	}

}
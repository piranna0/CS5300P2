import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;

public class EdgeElim {
	static double fromNetID = .94;
	static double rejectMin = .99 * fromNetID;
	static double rejectLimit = rejectMin + .01;
	
	public static void main(String[] args){
		try {
			createNewInputFile(args[0]);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	// assume 0.0 <= rejectMin < rejectLimit <= 1.0
	static boolean selectInputLine(double x) {
		return ( ((x >= rejectMin) && (x < rejectLimit)) ? false : true );
	}
	
	public static void createNewInputFile(String fileName) throws IOException{
		BufferedReader br = new BufferedReader(new FileReader(new File("edges.txt")));
		String line;
		PrintWriter writer = new PrintWriter(fileName);
		while((line = br.readLine()) != null){
			String[] str = line.trim().split("\\s+");
			if(selectInputLine(Double.parseDouble(str[2]))){
				writer.write(line + '\n');
				System.out.println(line);
			}
		}
		writer.close();
		br.close();
	}
}


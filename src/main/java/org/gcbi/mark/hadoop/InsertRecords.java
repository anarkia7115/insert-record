package org.gcbi.mark.hadoop;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;


public class InsertRecords {
	
	public static void main(String[] args) throws FileNotFoundException {
		
		PrintWriter out = new PrintWriter("/home/gcbi/workspace/insert-record/data/filename.txt");
		out.println("Hello first!");
		

		
		// the mapreduce job
		runMapReduce(args);
		
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
		Calendar cal = Calendar.getInstance();
		String currentTime = dateFormat.format(cal.getTime());
		
		String printString = "===========================Hello World!!!===========================";
		System.out.println(printString);
		
		String text = "Hello World!!! " + currentTime;
		
		out.println(text);
		
		out.close();

	}
	

	public static void runMapReduce(String[] args) {
		int res1 = 0;
		int res2 = 0;
		
		try {
			Configuration c = new Configuration();
			MapReduceRunner runner1 = new MapReduceRunner(org.gcbi.mark.hadoop.Mapper1.class);
			MapReduceRunner runner2 = new MapReduceRunner(org.gcbi.mark.hadoop.Mapper2.class);
			
			String path2_1 = "./insert-record/input"; 
			String path2_2 = "./insert-record/output2";
			String[] args2 = new String[2];
			args2[0] = path2_1;
			args2[1] = path2_2;
			res1 = ToolRunner.run(c, runner1, args);
			res2 = ToolRunner.run(c, runner2, args2);
		} catch (Exception ex) {
			System.err.println("Caught Exception: " + ex.getLocalizedMessage());
		}
		//System.exit(res);
	}

}

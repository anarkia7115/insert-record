package org.gcbi.mark.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;


public class InsertRecords {
	
	public static void main(String[] args) {
		// the mapreduce job
		runMapReduce(args);
	}

	public static void runMapReduce(String[] args) {
		int res = 0;
		try {
			Configuration c = new Configuration();
			MapReduceRunner runner = new MapReduceRunner();
			res = ToolRunner.run(c, runner, args);
		} catch (Exception ex) {
			System.err.println("Caught Exception: " + ex.getLocalizedMessage());
		}
		System.exit(res);
	}

}

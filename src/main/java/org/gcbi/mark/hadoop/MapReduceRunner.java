package org.gcbi.mark.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;


public class MapReduceRunner extends Configured implements Tool {

	Class<? extends Mapper> cls;
	String jobId;
    //@Override
    public int run(String[] strings) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Insert Record");

		job.setJarByClass(this.cls);
		
		job.setMapperClass(this.cls);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(strings[0]));
		FileOutputFormat.setOutputPath(job, new Path(strings[1]));
		
		int ret = runTimedJob(job, "Insert Record Job");
		
		return ret;
	}
	
    public MapReduceRunner(Class<? extends Mapper> mapCls) {
    	super();
    	
    	this.cls = mapCls;
    }
    
    protected int runTimedJob(Job job, String jobname) throws IOException, InterruptedException, ClassNotFoundException {

        //int ret = job.waitForCompletion(true) ? 0 : 1;
        job.submit();
        return 0;
    }
}

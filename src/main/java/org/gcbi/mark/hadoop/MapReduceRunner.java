package org.gcbi.mark.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;


public class MapReduceRunner extends Configured implements Tool {

    //@Override
    public int run(String[] strings) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Insert Record");

		job.setMapperClass(InsertRecordsMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(strings[0]));
		FileOutputFormat.setOutputPath(job, new Path(strings[1]));
		return 0;
	}
	
    protected int runTimedJob(Job job, String jobname) throws IOException, InterruptedException, ClassNotFoundException {

        int ret = job.waitForCompletion(true) ? 0 : 1;
        return ret;
    }
}

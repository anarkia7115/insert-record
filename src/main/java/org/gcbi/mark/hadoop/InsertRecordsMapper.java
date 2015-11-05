package org.gcbi.mark.hadoop;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class InsertRecordsMapper extends Mapper<Object, Text, Text, Text> {
	
	protected String jobId = new String();
	
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
		Calendar cal = Calendar.getInstance();
		String currentTime = jobId + dateFormat.format(cal.getTime());
		TimeUnit.MILLISECONDS.sleep(100);
		context.write(value, new Text(currentTime));
		
	}

}

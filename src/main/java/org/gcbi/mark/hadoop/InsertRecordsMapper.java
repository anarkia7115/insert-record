package org.gcbi.mark.hadoop;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class InsertRecordsMapper extends Mapper<Object, Text, Text, Text> {
	
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
		Calendar cal = Calendar.getInstance();
		String currentTime = dateFormat.format(cal.getTime());
		
		context.write(value, new Text(currentTime));
		
	}
}

package com.sist.mapred;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ApacheMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final IntWritable one=new IntWritable(1);
	private final Text res=new Text();
	
	static String regex;
	private Pattern p=Pattern.compile(regex);
	
	/*public ApacheMapper() {
		regex="(([0-9]{1,3})\\.([0-9]{1,3})\\.([0-9]{1,3})\\.([0-9]{1,3}))"; //ip서치
		System.out.println("안녕");
	}*/
	
	/*{
		regex="(([0-9]{1,3})\\.([0-9]{1,3})\\.([0-9]{1,3})\\.([0-9]{1,3}))"; //ip서치
		
	}*/
	
	static{
		regex="(([0-9]{1,3})\\.([0-9]{1,3})\\.([0-9]{1,3})\\.([0-9]{1,3}))"; //ip서치
		
	}
	
	
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
	
		Matcher m=p.matcher(value.toString());
		if (m.find()) {
			res.set(m.group());
			context.write(res, one);
		}
		
	}
	
	
	
	
	
	
	
	
}

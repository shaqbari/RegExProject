package com.sist.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ApacheDriver2 {
	public static void main(String[] args) {
		try {
			Configuration conf=new Configuration();
			Job job=Job.getInstance(conf,"apache-log");
			job.setMapperClass(ApacheMapper.class);
			//job.setMapperClass(new ApacheMapper().getClass());
			job.setReducerClass(ApacheReducer.class);
			job.setJarByClass(ApacheDriver.class);
			
			//결과값
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			//파일올리기
			FileInputFormat.addInputPath(job, new Path("/home/sist/access_log"));
			FileOutputFormat.setOutputPath(job, new Path("./output"));
			//결과값출력
			job.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

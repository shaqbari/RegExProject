package com.sist.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;


public class ApacheDriver {
	public static void main(String[] args) {
		try {
			Configuration conf=new Configuration();
			Job job=Job.getInstance(conf, "apache-log");
			job.setMapperClass(ApacheMapper.class);
			job.setReducerClass(ApacheReduecer.class);
			job.setJarByClass(ApacheDriver.class);
			
			//결과값 
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			//파일 올리기
			//FileInputFormat.addInputPath(job, new Path("/home/sist/access_log"));
			FileInputFormat.addInputPath(job, new Path("/home/sist/access_log"));
			//FileOutputFormat.			//FileOutputFormat.		
			//결과
			
			
			
			
			//실행
			
		} catch (Exception e) {
			// TODO: handle exception
		}
	}
}

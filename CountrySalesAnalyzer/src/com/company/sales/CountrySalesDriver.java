package com.company.sales;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// TASK-3 : Write a Map Reduce program to calculate the total units sold in each state for Onida company.
public class CountrySalesDriver {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Company Sales Analyzer");
		
		// Configure Mapper, Reducer and Driver classes
		job.setJarByClass(CountrySalesDriver.class);
		job.setMapperClass(CountrySalesMapper.class);
		job.setReducerClass(CountrySalesReducer.class);
		
		// Setup mapper output key and value types
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);	
		
		// Setup input file format
		job.setInputFormatClass(TextInputFormat.class);
		
		// Configure input and output file paths
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

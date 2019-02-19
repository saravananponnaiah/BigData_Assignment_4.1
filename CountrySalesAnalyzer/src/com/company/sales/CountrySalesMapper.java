package com.company.sales;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountrySalesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] tokens = value.toString().split("\\|");
		if (!tokens[0].equalsIgnoreCase("NA") && !tokens[1].equalsIgnoreCase("NA")) {
			// Filter records of company as Onida
			if (tokens[0].equalsIgnoreCase("Onida")) {
				context.write(new Text(tokens[3]), new IntWritable(1));
			}			
		}
	}
}

package com.company.sales;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CompanySalesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//StringTokenizer tokens = new StringTokenizer(value.toString().trim(), "|");
		String[] tokens = value.toString().split("\\|");
		if (!tokens[0].equalsIgnoreCase("NA") && !tokens[1].equalsIgnoreCase("NA")) {
			context.write(new Text(tokens[0]), new IntWritable(1));
		}
	}
}

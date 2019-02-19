package com.company.sales;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProductSalesMapper extends Mapper<LongWritable, Text, Text, Text>{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//StringTokenizer tokens = new StringTokenizer(value.toString().trim(), "|");
			String[] tokens = value.toString().split("\\|");
			if (!tokens[0].equalsIgnoreCase("NA") && !tokens[1].equalsIgnoreCase("NA")) {
				context.write(new Text("Summary"), value);
			}
	}
}

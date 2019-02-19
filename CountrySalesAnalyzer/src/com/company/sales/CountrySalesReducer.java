package com.company.sales;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountrySalesReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	public void reduce(Text company, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		context.write(company, new IntWritable(sum));
	}
}

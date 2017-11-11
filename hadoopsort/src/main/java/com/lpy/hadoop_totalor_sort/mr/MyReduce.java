package com.lpy.hadoop_totalor_sort.mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
	@Override
	protected void reduce(IntWritable key, Iterable<IntWritable> value,
			Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context)
			throws IOException, InterruptedException {
		int min = Integer.MIN_VALUE;
		int temp = 0;
		for (IntWritable v : value) {
		     temp =	v.get();
		     if (temp > min) {
		    	 min = temp;
			}
		}
		context.write(key, new IntWritable(min));
	}
}

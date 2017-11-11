package com.lpy.hadoop_second_sort.mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReduce extends Reducer<IntPair, NullWritable, IntWritable, IntWritable>{
	@Override
	protected void reduce(IntPair key, Iterable<NullWritable> value,
			Reducer<IntPair, NullWritable, IntWritable, IntWritable>.Context context)
			throws IOException, InterruptedException {
		int i = 0;
		for (NullWritable nullWritable : value) { // 此处的key随着value而变化 
			context.write(new IntWritable(key.getYear()), new IntWritable(key.getTemp()));
		}
		
	}
}

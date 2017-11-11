package com.lpy.hadoop_totalor_sort.mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context)
			throws IOException, InterruptedException {
	 String line =	value.toString();
	 String[] arr = line.split(" ");
	 context.write(new IntWritable(Integer.parseInt(arr[0])), new IntWritable(Integer.parseInt(arr[1])));
	}
}

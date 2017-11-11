package com.lpy.hadoop_second_sort.mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, IntPair, NullWritable>{
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, IntPair, NullWritable>.Context context)
			throws IOException, InterruptedException {
	 String line =	value.toString();
	 String[] arr = line.split(" ");
	 context.write(new IntPair(Integer.parseInt(arr[0]),Integer.parseInt(arr[1])),NullWritable.get());
	}
}

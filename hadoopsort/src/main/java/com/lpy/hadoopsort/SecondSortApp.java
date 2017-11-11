package com.lpy.hadoopsort;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.lpy.hadoop_second_sort.mr.IntPair;
import com.lpy.hadoop_totalor_sort.mr.GroupComparator;
import com.lpy.hadoop_totalor_sort.mr.KeyComparator;
import com.lpy.hadoop_totalor_sort.mr.MyMapper;
import com.lpy.hadoop_totalor_sort.mr.MyPartition;
import com.lpy.hadoop_totalor_sort.mr.MyReduce;

/**
 * 二次排序
 * @author 柳培岳
 *
 */
public class SecondSortApp {
	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance();

		job.setJarByClass(MyMapper.class);
		job.setJobName("Max temperature");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReduce.class);
		
		//设置分区器,排序,分组
		job.setPartitionerClass(MyPartition.class);
		job.setSortComparatorClass(KeyComparator.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		
		job.setMapOutputKeyClass(IntPair.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		//设置任务数
		job.setNumReduceTasks(3);
		job.waitForCompletion(true);
	}
}

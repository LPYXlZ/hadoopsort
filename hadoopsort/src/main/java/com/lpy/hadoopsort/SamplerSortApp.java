package com.lpy.hadoopsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler.RandomSampler;

import com.lpy.hadoop_totalor_sort.mr.MyMapper;
import com.lpy.hadoop_totalor_sort.mr.MyReduce;

import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

/**
 * 采样全排序
 * 
 * @author 柳培岳
 *
 */
public class SamplerSortApp {
	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();

		// 创建随机采样器
		RandomSampler<IntWritable, IntWritable> sampler = new RandomSampler<IntWritable, IntWritable>(0.1, 100, 3);
		//RandomSampler第一个参数表示key会被选中的概率，第二个参数是一个选取样本数，第三个参数是最大读取input数，这个数量必须和reduce数相同。
		
		//设置分区文件的位置
		TotalOrderPartitioner.setPartitionFile(configuration, new Path("file://home/ubuntu/data/par"));
		
		Job job = Job.getInstance(configuration);
		job.setJarByClass(MyMapper.class);
		job.setJobName("Max temperature");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReduce.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		//全排序分区类
		job.setPartitionerClass(TotalOrderPartitioner.class);
	
		//写partition file 到mapreduce，totalorderpartitioner,path
		InputSampler.writePartitionFile(job, sampler);
		
		// 设置任务数
		job.setNumReduceTasks(3);
		job.waitForCompletion(true);
	}
}

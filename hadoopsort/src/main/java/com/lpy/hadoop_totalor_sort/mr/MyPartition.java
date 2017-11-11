package com.lpy.hadoop_totalor_sort.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;


/**
 * 自定义分区器，决定key进入到那个reduce
 * @author 柳培岳
 *
 */
public class MyPartition extends Partitioner<IntWritable, IntWritable> {

	@Override
	public int getPartition(IntWritable key, IntWritable value, int numPartitions) {
		int year = key.get();
		int[][] arr = getParArr(numPartitions);
		return getIndex(year, arr);
	}

	private int[][] getParArr(int num) {
		int[][] map = new int[num][];

		int years = 10 / num;

		for (int i = 0; i < num; i++) {
			if (i == 0) {
				map[i] = new int[] { Integer.MIN_VALUE, 1901 + i };
			} else if (i == (num - 1)) {
				map[i] = new int[] { 1901 + ((i - 1) * years), Integer.MAX_VALUE };
			} else {
				map[i] = new int[] { 1901 + ((i - 1) * years), 1901 + (i * years) };
			}
		}
		return map;
	}

	private int getIndex(int year, int[][] arr) {
		for (int i = 0; i < arr.length; i++) {
			int min = arr[i][0];
			int max = arr[i][1];
			if (year >= min && year < max) {
				return i;
			}
		}
		return -1;

	}
}

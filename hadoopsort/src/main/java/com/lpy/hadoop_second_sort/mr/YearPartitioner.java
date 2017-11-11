package com.lpy.hadoop_second_sort.mr;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;


/**
 * 按照year来分区
 * @author 柳培岳
 *
 */
public class YearPartitioner extends Partitioner<IntPair, NullWritable>  {

	@Override
	public int getPartition(IntPair key, NullWritable value, int numPartitions) {
		int year= key.getYear();
		int [][] arr = getParArr(numPartitions);
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

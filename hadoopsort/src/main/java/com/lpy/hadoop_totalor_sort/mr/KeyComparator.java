package com.lpy.hadoop_totalor_sort.mr;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.lpy.hadoop_second_sort.mr.IntPair;

public class KeyComparator extends WritableComparator{
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		IntPair p1 = (IntPair) a;
		IntPair p2 = (IntPair) b;
		return p1.compareTo(p2);
	}
}

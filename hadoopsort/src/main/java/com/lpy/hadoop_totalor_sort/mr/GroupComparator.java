package com.lpy.hadoop_totalor_sort.mr;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.lpy.hadoop_second_sort.mr.IntPair;


/**
 * 分组对比器
 * @author 柳培岳
 *
 */
public class GroupComparator extends WritableComparator{
	
	public GroupComparator() {
		super(IntPair.class,true);
	}
	
	/**
	 * 
	 */
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		IntPair p1 = (IntPair) a;
		IntPair p2 = (IntPair) b;
		return p1.getYear() - p2.getYear();
	}
	
}

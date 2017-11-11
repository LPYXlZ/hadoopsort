package com.lpy.hadoop_second_sort.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


/**
 * 重新构造输出的key
 * @author 柳培岳
 *
 */
public class IntPair implements WritableComparable<IntPair>{

	private int year;
	private int temp;
	
	

	public IntPair() {
	}

	public IntPair(int year, int temp) {
		this.year = year;
		this.temp = temp;
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getTemp() {
		return temp;
	}

	public void setTemp(int temp) {
		this.temp = temp;
	}

	public void write(DataOutput out) throws IOException {

		out.write(year);
		out.write(temp);
	}

	public void readFields(DataInput in) throws IOException {
		year = in.readInt();
		temp = in.readInt();
	}

	
	/**
	 * year 升序， temp 降序
	 */
	
	public int compareTo(IntPair o) {
		int y = o.year ;
		int t = o.temp;
		if (this.year != y) {
			return this.year - y;
		}
		return t - this.temp;
	}

}

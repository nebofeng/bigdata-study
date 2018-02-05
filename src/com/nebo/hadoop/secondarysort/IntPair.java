package com.nebo.hadoop.secondarysort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class IntPair implements WritableComparable<IntPair>{
	
	private int first;
	private int second;
	public void set(int left, int right){
		first = left;
		second = right;
	}
	public int getFirst(){
		return first;
	}
	public int getSecond(){
		return second;
	}
	@Override
	//反序列化，从流中的二进制转换成IntPair
	public void readFields(DataInput in) throws IOException{
		first = in.readInt();
		second = in.readInt();
	}
	@Override
	//序列化，将IntPair转化成使用流传送的二进制
	public void write(DataOutput out) throws IOException{
		out.writeInt(first);
		out.writeInt(second);
	}
	@Override
	public int compareTo(IntPair o) {
		System.out.println("调用cmompare to "+ first);
		if(first!=o.first){
			return first < o.first ? -1 : 1;
		}else if(second != o.second){
			System.out.println("调用cmompare to "+ second);
			return second < o.second ? -1 : 1;
		}
		return 0;
	}
	
	@Override
	public int hashCode(){
		return first * 157 + second;
	}
	@Override
	public boolean equals(Object right){
		if (right == null)
			return false;
		if (this == right)
			return true;
		if (right instanceof IntPair){
			IntPair r = (IntPair) right;
			return r.first == first && r.second == second;
		}else{
			return false;
		}
	}

}

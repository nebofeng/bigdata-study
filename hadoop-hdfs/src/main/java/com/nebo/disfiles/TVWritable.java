package com.nebo.disfiles;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class TVWritable implements WritableComparable<Object> {
	
	private int bfCount;
	

	private int scCount;
	private int plCount;
	private int dzCount;
	private int cCount;
	
	public  TVWritable() {
		 
	}
	
	public TVWritable(int bfCount,int scCount,int plCount ,int dzCount,int cCount){
		this.bfCount = bfCount;
		this.scCount = scCount;
		this.plCount = plCount;
		this.dzCount = dzCount;
		this.cCount = cCount;	 		
	}
	
	public void set(int bfCount,int scCount,int plCount ,int dzCount,int cCount){
		this.bfCount = bfCount;
		this.scCount = scCount;
		this.plCount = plCount;
		this.dzCount = dzCount;
		this.cCount = cCount;	 		
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		bfCount =  in.readInt();
		scCount =  in.readInt();
		plCount =  in.readInt();
		dzCount =  in.readInt();
		cCount  =  in.readInt();
		 
	}

	@Override
	public void write(DataOutput out) throws IOException {
		  out.writeInt(bfCount); 
		  out.writeInt(scCount); 
		  out.writeInt(plCount); 
		  out.writeInt(dzCount); 
		  out.writeInt(cCount); 
		
	}

	@Override
	public int compareTo(Object arg0) {
		return 0;
	}
	
	
	public int getBfCount() {
		return bfCount;
	}

	public void setBfCount(int bfCount) {
		this.bfCount = bfCount;
	}

	public int getScCount() {
		return scCount;
	}

	public void setScCount(int scCount) {
		this.scCount = scCount;
	}

	public int getPlCount() {
		return plCount;
	}

	public void setPlCount(int plCount) {
		this.plCount = plCount;
	}

	public int getDzCount() {
		return dzCount;
	}

	public void setDzCount(int dzCount) {
		this.dzCount = dzCount;
	}

	public int getcCount() {
		return cCount;
	}

	public void setcCount(int cCount) {
		this.cCount = cCount;
	}
	@Override
	public String toString() {
		 
		return bfCount+" "+scCount+" "+" "+plCount+" "+dzCount+" "+cCount; 
	 
	}
}

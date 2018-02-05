package com.nebo.hadoop.mapmultiinputformat;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
/**
* 学习成绩读写类
* 数据格式参考：19020090017 小讲 90 99 100 89 95
* @author Bertron
*/
public class ScoreWritable implements WritableComparable< Object > {
	private float Chinese;
	private float Math;
	private float English;
	private float Physics;
	private float Chemistry;
	public ScoreWritable(){     
	}
	public ScoreWritable(float Chinese,float Math,float English,float Physics,float Chemistry){
	    this.Chinese = Chinese;
	    this.Math = Math;
	    this.English = English;
	    this.Physics = Physics;
	    this.Chemistry = Chemistry;
	}
	public void set(float Chinese,float Math,float English,float Physics,float Chemistry){
	    this.Chinese = Chinese;
	    this.Math = Math;
	    this.English = English;
	    this.Physics = Physics;
	    this.Chemistry = Chemistry;
	}
    public float getChinese() {
        return Chinese;
    }
    public float getMath() {
        return Math;
    }
    public float getEnglish() {
        return English;
    }
    public float getPhysics() {
        return Physics;
    }
    public float getChemistry() {
        return Chemistry;
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        Chinese = in.readFloat();
        Math = in.readFloat();
        English = in.readFloat();
        Physics = in.readFloat();
        Chemistry = in.readFloat();
    }
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeFloat(Chinese);
        out.writeFloat(Math);
        out.writeFloat(English);
        out.writeFloat(Physics);
        out.writeFloat(Chemistry);
    }
    @Override
    public int compareTo(Object o) {
        return 0;
    }
}
package com.nebo.homework.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

public class Min extends UDF{
	
	
	public Double evaluate(Double a, Double b) {
		
		if(a==null)
			a=0.0;
		if(b==null)
			b=0.0;
		if(a>=b){
			return b;
		}else{
			return a;
		}
	}

}

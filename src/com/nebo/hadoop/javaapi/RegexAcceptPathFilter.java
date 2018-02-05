package com.nebo.hadoop.javaapi;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

//过滤文件
public class RegexAcceptPathFilter implements PathFilter {
	private final String regex;
	
	public RegexAcceptPathFilter(String regex) {
		this.regex = regex;
	}
	
	@Override
	public boolean accept(Path path) {
		// TODO Auto-generated method stub
		boolean flag = path.toString().matches(regex);
	 
		return flag;
	}
}
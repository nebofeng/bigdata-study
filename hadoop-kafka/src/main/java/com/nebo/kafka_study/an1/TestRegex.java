package com.nebo.kafka_study.an1;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestRegex {

	
	 public static void main(String[] args) {
		  String string = "{\"userId\":2017,\"day\":\"2017-03-02\",\"begintime\":1488326400000,\"endtime\":1488327000000,\"data\":[{\"package\":\"com.browser\",\"activetime\":120000}]}";
		  String regex = ":(\\\\d+|\\w+)";
		  Pattern p = Pattern.compile(regex);
		  Matcher m =p.matcher(string);
		  if(m.find()) {
			  
			  System.out.println(m.group(1).toString());
		  }
		  
	}
}

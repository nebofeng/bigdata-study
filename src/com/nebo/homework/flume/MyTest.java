package com.nebo.homework.flume;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
 
import org.apache.commons.lang3.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyTest implements Interceptor {
	 private static final Logger logger = LoggerFactory  
	            .getLogger(MyTest.class);  
	@Override
	public void initialize() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Event intercept(Event event) {
		Pattern p = Pattern.compile("(.*)\\.(.*)\\.(.*)",Pattern.DOTALL);
		
		Map<String, String> headers = event.getHeaders();  
		String name= headers.get("basename");
		logger.info( "=====basename》"+name);
		logger.debug(  
                "=====basename》"+name
                 );  
		if(!StringUtils.isEmpty(name)) {
			
			Matcher m = p.matcher(name);
			
			if(m.find()) {
				String type = m.group(1).toString().trim();
				String filename = m.group(3).toString().trim();
				logger.info( "=====type》"+type);
				logger.debug(  
		                "=====filename》"+filename
		                 );  
				headers.put("type", type);
				headers.put("name", filename);
			}else {
				logger.info( "=====type》");
			}
			
		}else {
			headers.put("type", "type");
			headers.put("name", "name");
		}
		
		 
		return event;
	}

	@Override
	public List<Event> intercept(List<Event> events) {
		List intercepted = new ArrayList<>(events.size());
		for (Event event : events) {
		Event interceptedEvent = intercept(event);
		if (interceptedEvent != null) {
		intercepted.add(interceptedEvent);
		}
		}
		return intercepted;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}
	
	  public static class Builder implements Interceptor.Builder{

		@Override
		public void configure(Context context) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public Interceptor build() {
			// TODO Auto-generated method stub
			return new MyTest();
		}
		  
	  }

}

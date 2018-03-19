package com.nebo.homework.kafka.an1;

import java.util.Properties;



import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class TestProducer    {
	String topic = "demotest";
	Producer<String, String> producer ;
	 
	public TestProducer() {
	 
	 		
		Properties props = new Properties();

		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("metadata.broker.list",

		"192.168.11.121:9092,192.168.11.122:9092,192.168.11.123:9092");
		 
		props.put("partitioner.class", "com.nebo.homework.kafka.TestPartitioner");

		props.put("request.required.acks", "1");	 
		
	    producer = new Producer<String, String>(new ProducerConfig(props));
		
		
	}
	
	public void sendMessgae(String key ,String value ) {
		producer.send(new KeyedMessage<String, String>(topic, key, value));
		
	 		
	}
	
	public void close(){
		producer.close();
	}
	 
			 
}

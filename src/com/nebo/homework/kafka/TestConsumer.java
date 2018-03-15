package com.nebo.homework.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class TestConsumer {
	
	public static void main(String[] args) {
         
		 String topic = "demotest";

	     //配置文件
	     Properties properties = new Properties();
	     properties.put("group.id", "neboGroup");
	   //  properties.put("bootstrap.servers", "192.168.11.121:9092,192.168.11.122:9092,192.168.11.123:9092");
	     properties.put("zookeeper.connect", "192.168.11.121:2181,192.168.11.122:2181,192.168.11.123:2181");
	     properties.put("auto.offset.reset", "largest");
	     properties.put("auto.commit.interval.ms", "1000");

	//   properties.put("value.serializer",
//	       "org.apache.kafka.common.serialization.StringSerializer");
	//   properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

	     //设置消费者的配置文件
	     ConsumerConfig config = new ConsumerConfig(properties);

	     //创建连接器
	     ConsumerConnector conn = Consumer.createJavaConsumerConnector(config);

	     //key为topic   value为partition的个数
	     Map<String, Integer> map = new HashMap<String, Integer>();
	     

	     //封装对应消息的的topic和partition个数
	     map.put(topic, 3);

	     //获取partition的流, key为对应的topic名字,value为每个partition的流，这里有三个partiiton所以list里面有三个流
	     Map<String, List<KafkaStream<byte[], byte[]>>> createMessageStreams = conn
	         .createMessageStreams(map );
	     

	     //取出对应topic的流的list
	     List<KafkaStream<byte[], byte[]>> list = createMessageStreams.get(topic);

	     //用线程池创建3个对应的消费者
	     ExecutorService executor = Executors.newFixedThreadPool(3);

	     //执行消费
	     for (int i = 0; i < list.size(); i++) {
	         executor.execute(new TestConsumerThread("消费者" + (i + 1), list.get(i)));
	     }
	
	
	}		
	
}

package com.nebo.kafka_study.an1;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class TestProducer    {
	static  String topic = "demo";
	Producer<String, String> producer ;
	static Random random =new Random();
	 
	public TestProducer() {
	 
	 		
		Properties props = new Properties();

		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("metadata.broker.list", "139.199.172.112:9092");
        props.put("zookeeper.connect", "139.199.172.112:2181");

        props.put("partitioner.class", "com.nebo.kafka_study.an1.TestPartitioner");

		props.put("request.required.acks", "1");

	    producer = new Producer<String, String>(new ProducerConfig(props));


	}

	public void sendMessgae(String key ,String value ) {
	    System.out.println("topic="+topic+" key="+key+" value="+value);
		producer.send(new KeyedMessage<String, String>(topic, key, value));


	}

	public void close(){
		producer.close();
	}

	public static void main(String[] args) {
		TestProducer testProducer = new TestProducer();
		while (true){
			//模拟message
			String value = UUID.randomUUID().toString();
			int k = random.nextInt(3);
			String[] keys ={"1000","2000","3000"};

			//封装message,这个是另一种使用方法。
			ProducerRecord<String, String> pr = new ProducerRecord<String, String>(topic, value);

			//发送消息
			testProducer.sendMessgae(keys[k],value);

			//sleep
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	 
			 
}

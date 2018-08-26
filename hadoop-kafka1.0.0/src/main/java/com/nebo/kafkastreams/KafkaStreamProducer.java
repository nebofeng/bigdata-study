package com.nebo.kafkastreams;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.SettingUtil;

import java.util.Properties;

public class KafkaStreamProducer {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamProducer.class);

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", SettingUtil.getKey("","txynebo19092"));
        props.put("acks", "all");
        props.put("retries", 1);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 1000000; i++) {
            String message = "这是 一条 测试 消息 , 编号:";
            producer.send(new ProducerRecord<String, String>("test", Integer.toString(i), message));
            LOG.info("测试发送消息: {}", message);
            System.out.println(i);
            Thread.sleep(1000);
        }

        producer.close();
    }
}

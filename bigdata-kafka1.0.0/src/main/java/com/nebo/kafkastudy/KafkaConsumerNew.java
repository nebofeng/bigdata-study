package com.nebo.kafkastudy;

import org.apache.kafka.clients.consumer.*;
import utils.SettingUtil;

import java.util.Arrays;
import java.util.Properties;

;

public class KafkaConsumerNew {
    private Consumer<String,String> consumer;
    private static String group = "group-1";
    private static String TOPIC ="test";

    private KafkaConsumerNew(){
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SettingUtil.getKey("","txynebo39092"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
       // props.put("zookeeper.connect", SettingUtil.getKey("","txynebo12181"));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"); // 自动commit
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000"); // 自动commit的间隔
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<String, String>(props);

    }

    private void consume(){
        consumer.subscribe(Arrays.asList(TOPIC));

        while(true ){
            ConsumerRecords<String ,String> records =consumer.poll(1000);


            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s \n", record.offset(), record.key(), record.value());
                try {

                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }

    }

    public static void main(String[] args) {

        for (int i=0;i<3;i++) {
            new Thread(()->new KafkaConsumerNew().consume()).start();

        }
    }

}

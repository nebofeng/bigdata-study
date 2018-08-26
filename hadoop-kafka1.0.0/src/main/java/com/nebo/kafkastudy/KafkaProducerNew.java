package com.nebo.kafkastudy;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import utils.SettingUtil;

import java.util.Properties;

public class KafkaProducerNew {
    private final KafkaProducer<String ,String> producer;
    public  final static String Topic ="test";
    public KafkaProducerNew() {
    //构造方法中初始化 kafka producer 参数配置
        Properties proes=new Properties();
         proes.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                 SettingUtil.getKey("","txynebo19092")+","
                 +SettingUtil.getKey("","txynebo29092"));
        proes.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        proes.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
       // proes.put("zookeeper.connect", SettingUtil.getKey("","txynebo12181"));
        producer = new KafkaProducer<String, String>(proes);
    }

    public void produce(){
        int messgeNo =1 ;
        final int Count =10;
        while(messgeNo< Count){

            String key =String.valueOf(messgeNo);
            String data=String.format("hello kafaka producer message %s",key);
            try{

                Thread.sleep(1000);
                producer.send(new ProducerRecord<String, String>(Topic,key ,data));
                messgeNo++;
            }catch (Exception e){
                e.printStackTrace();
            }


        }
        producer.close();

    }

    public static void main(String[] args) {
        new KafkaProducerNew().produce();
    }
}

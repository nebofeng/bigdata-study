package pers.nebo.kafkaoffset;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * @ author fnb
 * @ email nebofeng@gmail.com
 * @ date  2020/1/1
 * @ des : 获取kafka的最新offset
 */
public class KafkaGetLastOffset {


    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "node1:6667");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaConsumer kafkaConsumer = new KafkaConsumer(props);
        List<TopicPartition> tp =new ArrayList<TopicPartition>();

        List<PartitionInfo> partitionInfoList=kafkaConsumer.partitionsFor("test");
        for(PartitionInfo partitionInfo:partitionInfoList){
            tp.add(new TopicPartition("test",partitionInfo.partition()));
        }
        kafkaConsumer.assign(Collections.singletonList(tp));
        kafkaConsumer.seekToEnd(Collections.singletonList(tp));

        for(TopicPartition topicPartition:tp){

            System.out.println("Partition " + topicPartition.partition()
            + " 's latest offset is '" + kafkaConsumer.position(topicPartition));

        }






    }
}

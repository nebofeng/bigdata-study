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

        String topic="test-topic";

        Properties props = new Properties();
        props.put("bootstrap.servers", "node1:6667");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaConsumer kafkaConsumer = new KafkaConsumer(props);
        List<TopicPartition> tps =new ArrayList<TopicPartition>();

        List<PartitionInfo> partitionInfoList=kafkaConsumer.partitionsFor("test");
        for(PartitionInfo partitionInfo:partitionInfoList){
            tps.add(new TopicPartition(topic,partitionInfo.partition()));
        }


        /*
         method1  参考博客做了一些修改，修改了认为不对的地方。
         https://www.cnblogs.com/cocowool/p/get_kafka_latest_offset.html
        */
       method1(topic,props,tps);

      /*
        method 2
        https://blog.csdn.net/chenggongdeli11/article/details/93502206
      */

       method2(topic,props,tps);

     }

    /**
     * by seek to end method
     * @param topic
     * @param props
     * @param tps
     */
     public static  void method1(String topic,Properties props,List <TopicPartition> tps){
        KafkaConsumer kafkaConsumer = new KafkaConsumer(props);
        kafkaConsumer.assign(Arrays.asList(topic));
        kafkaConsumer.seekToEnd(tps);
        for(TopicPartition topicPartition:tps){
            System.out.println("Partition " + topicPartition.partition()
                    + " 's latest offset is '" + kafkaConsumer.position(topicPartition));

        }

    }

    /**
     * by endoffsets method
     * @param topic
     * @param props
     * @param tps
     */

    public static void method2(String topic,Properties props,List <TopicPartition> tps) {
        KafkaConsumer kafkaConsumer = new KafkaConsumer(props);

        Map<TopicPartition,Long> topicPartitionOffsets=kafkaConsumer.endOffsets(tps);
        for(Map.Entry entry:topicPartitionOffsets.entrySet()){
            System.out.println(entry.getKey()+"=="+entry.getValue());

        }

    }
}

package com.nebo.kafkatohdfs;

import com.nebo.hdfs_utils.HdfsUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import utils.SettingUtil;

import java.util.Arrays;
import java.util.Properties;

public class ConsumerRunnable implements Runnable {

    // 每个线程维护私有的KafkaConsumer实例
    private final KafkaConsumer<String, String> consumer;
    private String topic;

    public ConsumerRunnable(String brokerList, String groupId, String topic) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokerList);
        props.put("group.id", groupId);
        // props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "false");        //本例使用手动提交位移
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        this.consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));   // 本例使用分区副本自动分配策略
        this.topic = topic;
    }

    @Override
    public void run() {
        boolean flag = true;
        int minBatchSize = 0;
        String fileName = null;
        FSDataOutputStream outputStream = null;
        while (true) {
            System.out.println("while true ====进入" + System.currentTimeMillis());

            ConsumerRecords<String, String> records = consumer.poll(2000);// 本例使用20000ms作为获取超时时间
            System.out.println(" pool ......." + System.currentTimeMillis());
            System.out.println("count========" + records.count());
            try {
                for (ConsumerRecord<String, String> record : records) {

                    if (flag == true) {
                        int par = record.partition();
                        long offset = record.offset();
                        //create file
                        fileName = "/test/" + topic + "_" + par + "_" + offset + ".log";
                        System.out.println("filename=============" + fileName);
                        outputStream = HdfsUtils.createFile(fileName);
                        flag = false;
                    }
                    System.out.println("key ===" + record.key() + "value===" + record.value() + "offser===" + record.offset());
                    outputStream.writeChars(record.value() + "=======bbbb");
                    minBatchSize++;
                }
                //至少5条消息提交一个文件。
                if (minBatchSize >= 5) {
                    System.out.println("size ====" + minBatchSize);


                    boolean result = true;
                    try {
                        outputStream.flush(); //抛出异常之后如果有try ，就不会停止。
                        outputStream.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                        result = false;
                    }
                    if (result == true) {
                        consumer.commitSync();
                        minBatchSize = 0;
                        flag = true;
                        fileName = null;
                        outputStream = null;
                        System.out.println("commit ------------------------");
                    } else {
                        // flag=true;
                        //执行失败之后，再次进入while 循环。如果flag为false；consumer pool 会获取不到数据 。
                        System.out.println("======error");
                        consumer.close();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    public static void main(String[] args) {
//
//        String fileName = "/test/"+"topic"+"_"+"par"+"_"+"offset"+".log";
//
//        HdfsUtils.createFile(fileName);

        String brokerList = SettingUtil.getKey("", "txynebo19092");
        String groupId = "testGroup1";
        String topic = "test";
        int consumerNum = 1;
        ConsumerRunnable consumerThread = new ConsumerRunnable(brokerList, groupId, topic);
        consumerThread.test();


    }

    void test(){
        ConsumerRecords<String, String> myRecords = null;
        while (true) {
            System.out.println("while true ====进入" + System.currentTimeMillis());
//            TopicPartition p = new TopicPartition("test",0);
//            consumer.assign(Arrays.asList(p));
//            consumer.seek(p,79233);
//
//       //     consumer.seek(new TopicPartition("test",0),79233);
            ConsumerRecords<String, String> records = consumer.poll(2000);// 本例使用20000ms作为获取超时时间

            if(records.count()!=0){

                 myRecords = records;
            }
            
            System.out.println(" pool ......." + System.currentTimeMillis());
            System.out.println("count========" + records.count());

            for (ConsumerRecord<String, String> record : myRecords) {
                System.out.println("offset========" + record.offset());
            }
            


        }

    }
}



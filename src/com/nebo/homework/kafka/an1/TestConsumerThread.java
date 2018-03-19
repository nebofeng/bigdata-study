package com.nebo.homework.kafka.an1;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

public class TestConsumerThread implements Runnable {
	//当前消费者的名字
    private String consumerName;

    //当前消费者的流
    private KafkaStream<byte[], byte[]> stream;

    //构造函数
    public TestConsumerThread(String consumerName, KafkaStream<byte[], byte[]> stream) {
        super();
        this.consumerName = consumerName;
        this.stream = stream;
    }

    @Override
    public void run() {

        //获取当前数据的迭代器
        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();

        //消费数据
        while (iterator.hasNext()) {

            //取出消息
            MessageAndMetadata<byte[], byte[]> next = iterator.next();

            //获取topic名字
            String topic = next.topic();

            //获取partition编号
            int partitionNum = next.partition();

            //获取offset
            long offset = next.offset();

            //获取消息体
            String message = new String(next.message());

            //测试打印
            System.out
                .println("consumerName: "+ consumerName + "topic: " + topic + " ,partitionNum: "
                            + partitionNum + " ,offset: " + offset + " ,message: " + message);
        }
    }
}

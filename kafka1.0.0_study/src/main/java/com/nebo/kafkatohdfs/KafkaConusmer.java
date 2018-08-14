package com.nebo.kafkatohdfs;

import utils.SettingUtil;

/**
 * kafka 消费者  write file to hdfs
 */
public class KafkaConusmer {
    //初始化配置
    static {

    }

    /* 多线程批量写入 hdfs 数据 ， 1.
    * 1、 创建初始文件 以及偏移量。
    * 记录文件目录文件名，文件偏移量。
    *
    *
    * 2、 上传成功，提交偏移量
    * 3、创建初始文件 以及偏移量。
    * 更改文件目录文件名，文件偏移量
    * */
    public static void main(String[] args) {

        String brokerList = SettingUtil.getKey("","txynebo19092");
        String groupId = "testGroup1";
        String topic = "test";
        int consumerNum = 1;
        ConsumerGroup consumerGroup = new ConsumerGroup(consumerNum, groupId, topic, brokerList);
        consumerGroup.execute();



    }
}

package com.nebo.kafkatohdfs;

import com.nebo.hdfs_utils.HdfsUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import utils.SettingUtil;

import java.util.*;

public class ConsumerRunnable implements Runnable {

  // 每个线程维护私有的KafkaConsumer实例
  private final KafkaConsumer<String, String> consumer;
  private String topic;

  public ConsumerRunnable(String brokerList, String groupId, String topic) {
    Properties props = new Properties();
    props.put("bootstrap.servers", brokerList);
    props.put("group.id", groupId);
    // props.put("auto.offset.reset", "earliest");
    props.put("enable.auto.commit", "false"); // 本例使用手动提交位移
    props.put("auto.commit.interval.ms", "1000");
    props.put("session.timeout.ms", "30000");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    this.consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Arrays.asList(topic)); // 本例使用分区副本自动分配策略
    this.topic = topic;
  }

  @Override
  public void run() {
    boolean flag = true;
    int minBatchSize = 0;
    String fileName = null;
    FSDataOutputStream outputStream = null;
    Map<String, String> currRecords = null;
    System.out.println("run ===========");
    while (true) {
      System.out.println("while true ====进入" + System.currentTimeMillis());
      try {

      	 System.out.println("falg====="+flag+" minsize==="+minBatchSize );
        if (flag == true || minBatchSize < 5) {
          // 当文件flag为false的时候 ，即上一次的文件未成功提交。再次pool，将会获取。上一次pool之后的记录。
          // 所以只有为true的时候(文件成功写入)，或者 文件批次还不够写入 才pool
          ConsumerRecords<String, String> records = consumer.poll(2000); // 本例使用20000ms作为获取超时时间


          for (ConsumerRecord<String, String> record : records) {
            // flag 标识是否，以当前offset新建文件。只有当前文件提交之后。才能新建文件 。flag也可以作为文件最终写入hdfs的标识。

	          if (flag == true) {
              int par = record.partition();
              long offset = record.offset();

              // create file
              fileName = "/test/" + topic + "_" + par + "_" + offset + ".log";
              System.out.println("filename=============" + fileName);
              outputStream = HdfsUtils.createFile(fileName);
              flag = false;
              currRecords=new HashMap<String,String>();

            }
            System.out.println(  "key ==="  + record.key()  + record.value()  + "offser==="   + record.offset());
            outputStream.writeChars(record.value() + "=======bbbb"+"\n");
            currRecords.put(String.valueOf(record.offset()),record.toString()) ;
            minBatchSize++;

          }

        } else {
          System.out.println("重复执行上一次未消费的数据=========");
          Iterator iter = currRecords.entrySet().iterator();
          outputStream = HdfsUtils.createFile(fileName);

          while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry) iter.next();
            Object key = entry.getKey();
            Object val = entry.getValue();
            outputStream.writeChars("key=="+key +" value"+val +"\n");
          }

      }

        // 至少5条消息提交一个文件。
        if (minBatchSize >= 5) {
          System.out.println("size ====" + minBatchSize);
          boolean result = true;
          try{
	          outputStream.flush(); // 抛出异常之后如果有try ，就不会停止。
	          outputStream.close();
	          System.out.println("==================未有异常。");
          }catch (Exception e){
          	e.printStackTrace();
          	result=false;


          }
         if(result==true){
	         // 如果没有执行成功。会被下面的catch 捕获。
	         consumer.commitSync();
	         minBatchSize = 0;
	         flag = true;
	         fileName = null;
	         outputStream = null;
	         System.out.println("commit ------------------------");
         }

        }
       } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public static void main(String[] args) {

    String brokerList = SettingUtil.getKey("", "txynebo19092");
    String groupId = "testGroup1";
    String topic = "test";
    int consumerNum = 1;
    ConsumerRunnable consumerThread = new ConsumerRunnable(brokerList, groupId, topic);
    consumerThread.test();
  }

  void test() {
    ConsumerRecords<String, String> myRecords = null;
    while (true) {
      System.out.println("while true ====进入" + System.currentTimeMillis());
      ConsumerRecords<String, String> records = consumer.poll(2000); // 本例使用20000ms作为获取超时时间

      if (records.count() != 0) {
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

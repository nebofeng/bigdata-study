package com.nebo.kafkastreams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.SettingUtil;

import java.util.Properties;

/**
 * WORDCOUNT 高级DSL API
 */
public class WordCountProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(WordCountProcessor.class);

    private static final String topic = "test";

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, SettingUtil.getKey("","txynebo29092"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());



        //创建流
        final StreamsBuilder builder = new StreamsBuilder();

        Topology topology = builder.build();

        topology.addSource("source","streams-plaintext-input");
//        topology.addProcessor("oneprocessor", ()-> new MyProcessorA(), "source");
//
//      topology.addProcessor("tworocessor", ()-> new MyProcessorB(), "source");
        topology.addProcessor("processor", ()-> new MyProcessor(), "source");
        topology.addProcessor("hdfsprocessor", ()-> new MyProcessorA(), "source");

        topology.addSink("one","streams-wordcount-output","processor");
        topology.addSink("two","streams-wordcount-output2","processor");


        KafkaStreams streams = new KafkaStreams(topology, props);

        streams.start();

    }

    static class MyProcessorA implements Processor<String,String>{
        private ProcessorContext context;
        @Override
        public void init(ProcessorContext context) {
            this.context= context;
        }

        @Override
        public void process(String key, String value) {

              if(value.contains("A")){
                  System.out.println("topic==="+context.topic()+"     key ========>"+key +"  value =========>"+value);
                  System.out.println("提交到A");
                  //TOOD
                  context.commit();
                  context.forward(key,value);
              }

        }

        @Override
        public void punctuate(long timestamp) {
               context.commit();
        }

        @Override
        public void close() {

        }
    }


    static class MyProcessorB implements Processor<String,String>{
        private ProcessorContext context;
        @Override
        public void init(ProcessorContext context) {
            this.context= context;
        }

        @Override
        public void process(String key, String value) {

            if(value.contains("B")){
                System.out.println("topic==="+context.topic()+"     key ========>"+key +"  value =========>"+value);
                System.out.println("提交到B");
                context.forward(key,value);

                context.commit();

            }
        }

        @Override
        public void punctuate(long timestamp) {

        }

        @Override
        public void close() {

        }
    }
    static class MyProcessor implements Processor<String,String>{
        private ProcessorContext context;
        @Override
        public void init(ProcessorContext context) {
            this.context= context;
        }

        @Override
        public void process(String key, String value) {


            //TODO: 写入到hdfs

            if(value.contains("B")){
                System.out.println("topic==="+context.topic()+"     key ========>"+key +"  value =========>"+value);
                System.out.println("提交到B");
                context.commit();
                context.forward(key,value,"two");
            }else if(value.contains("A")){
                System.out.println("topic==="+context.topic()+"     key ========>"+key +"  value =========>"+value);
                System.out.println("提交到A");
                context.commit();
                context.forward(key,value,"one");
            }
        }

        @Override
        public void punctuate(long timestamp) {

        }

        @Override
        public void close() {

        }
    }

}

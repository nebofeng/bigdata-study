package com.nebo.kafkastreams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import utils.SettingUtil;

import java.util.Properties;

public class Pipe {

    public static void main(String[] args)   {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, SettingUtil.getKey("","txynebo29092"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());


        //创建流
        final StreamsBuilder builder = new StreamsBuilder();

//        KStream<String, String> source = builder.stream("streams-plaintext-input");
//
//        source.to("streams-pipe-output");
        //填充流
        builder.stream("streams-test-input").to("streams-pipe-output");

        //流的源头 。拓扑结构
        final Topology topology = builder.build();

        System.out.println(topology.describe());

//        //表结构
//        final KafkaStreams streams = new KafkaStreams(topology, props);
//
//        final CountDownLatch latch = new CountDownLatch(1);
//
//        // attach shutdown handler to catch control-c
//        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
//            @Override
//            public void run() {
//                streams.close();
//                latch.countDown();
//            }
//        });
//
//        try {
//            streams.start();
//            latch.await();
//        } catch (Throwable e) {
//            System.exit(1);
//        }
//        System.exit(0);


    }
}
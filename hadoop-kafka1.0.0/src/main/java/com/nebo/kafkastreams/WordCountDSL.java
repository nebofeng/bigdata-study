package com.nebo.kafkastreams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import utils.SettingUtil;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class WordCountDSL {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, SettingUtil.getKey("","txynebo29092"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        //创建流
        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> source = builder.stream("streams-plaintext-input");


        source.foreach((key, value) ->  System.out.println("key========="+key+"value============="+value));

        source.filter((key, value) -> value.contains("A"))

                .to("streams-wordcount-output",Produced.with(Serdes.String(),Serdes.String()));

        source.filter((key, value) -> value.contains("B"))

                .to("streams-wordcount-output2",Produced.with(Serdes.String(),Serdes.String()));


//        source.flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
//
//                .groupBy(((key, value) -> value))
////                .count()
//                .count(Materialized.<String,Long,KeyValueStore<Bytes,byte[]>>as("counts-store"))
//                .toStream()
//                .to("streams-wordcount-output", Produced.with(Serdes.String(),Serdes.Long()));

        //流的源头 。拓扑结构
        final Topology topology = builder.build();



        System.out.println(topology.describe());

        //表结构
        final KafkaStreams streams = new KafkaStreams(topology, props);

        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);








    }
}

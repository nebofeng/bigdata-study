package com.nebo.kafkastreams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.SettingUtil;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;

/**
 * WORDCOUNT 高级DSL API
 */
public class KafkaStreamWordCountDSL {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamWordCountDSL.class);

    private static final String topic = "test";

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, SettingUtil.getKey("","txynebo19092"));
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5);

        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, String> source = builder.stream(topic);

        KTable<String, Long> counts = source.flatMapValues(new ValueMapper<String, Iterable<String>>() {
            @Override
            public Iterable<String> apply(String value) {
                return Arrays.asList(value.toLowerCase(Locale.getDefault()).split(" "));
            }
        }).map(new KeyValueMapper<String, String, KeyValue<String, String>>() {
            @Override
            public KeyValue<String, String> apply(String key, String value) {
                LOG.info("{} : {}", key, value);
                return new KeyValue<>(value, value);
            }
        }).groupByKey().count("Counts");

        counts.to(Serdes.String(), Serdes.Long(), topic);

        counts.writeAsText("E:/ttt", Serdes.String(), Serdes.Long());


        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();


        Thread.sleep(50000000L);


        streams.close();

    }
}

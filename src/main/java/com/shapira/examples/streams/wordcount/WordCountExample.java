package com.shapira.examples.streams.wordcount;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

public class WordCountExample {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, String> source =
                builder.stream("wordcount-input");
        final Pattern pattern = Pattern.compile("\\W+");
        KStream counts = source
                .flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))
                .map((key, value) -> new KeyValue<Object, Object>(value, value))
                .filter((key, value) -> (!value.equals("the")))
                .groupByKey()
                .count("CountStore")
                .mapValues(value -> Long.toString(value)).toStream();
        counts.to("wordcount-output");

        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();
        // usually the stream application would be running forever,
        // in this example we just let it run for some time and stop since the input data is finite.
        Thread.sleep(5000L);
        streams.close();
    }

}

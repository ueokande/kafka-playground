package com.shapira.examples.streams.wordcount;

import com.ueokande.kafka1.LocalKafkaServer;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Properties;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

public class WordCountExampleTest {

    private LocalKafkaServer server;

    @Rule
    public TemporaryFolder zkDataDir = new TemporaryFolder();

    @Rule
    public TemporaryFolder kafkaDataDir = new TemporaryFolder();

    @Before
    public void before() throws Exception {
        FileUtils.forceDelete(new File("/tmp/kafka-streams/wordcount"));
        server = new LocalKafkaServer(zkDataDir.getRoot(), kafkaDataDir.getRoot());
        server.start();
    }

    @After
    public void after() {
        server.close();
    }

    @Test
    public void run() throws Exception {
        String[] source = {
                "Hello world",
                "Here is a red apple and orange orange and yellow banana",
                "I like an apple and banana",
                "An apple is a most delicious fruit",
                "APPLE and BANANA"
        };

        try (Producer<String, String> producer = new KafkaProducer<>(producerConfig())) {
            for (String text : source) {
                ProducerRecord<String, String> r = new ProducerRecord<>("wordcount-input", text);
                producer.send(r);
            }
        }

        WordCountExample.main(new String[]{});

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig());
        consumer.subscribe(Pattern.compile("wordcount-output"));
        ConsumerRecords<String, String> records;
        while (true) {
            records = consumer.poll(5000);
            if (records.isEmpty()) {
                continue;
            }
            consumer.commitSync();
            break;
        }

        assertThat(records.records("wordcount-output")).anySatisfy(r -> {
            assertThat(r.key()).isEqualTo("apple");
            assertThat(r.value()).isEqualTo("4");
        }).anySatisfy(r -> {
            assertThat(r.key()).isEqualTo("orange");
            assertThat(r.value()).isEqualTo("2");
        }).anySatisfy(r -> {
            assertThat(r.key()).isEqualTo("banana");
            assertThat(r.value()).isEqualTo("3");
        });
    }

    private static Properties producerConfig() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    private static Properties consumerConfig() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-test");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return props;
    }


}
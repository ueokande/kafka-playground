package com.ueokande.kafka1;

import com.google.common.primitives.Longs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Properties;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

public class Kafka100Test {

    private LocalKafkaServer server;

    @Rule
    public TemporaryFolder zkDataDir = new TemporaryFolder();

    @Rule
    public TemporaryFolder kafkaDataDir = new TemporaryFolder();

    @Before
    public void before() throws Exception {
        server = new LocalKafkaServer(zkDataDir.getRoot(), kafkaDataDir.getRoot());
        server.start();
    }

    @After
    public void after() {
        server.close();
    }

    @Test
    public void kafkaHeaderTest() throws Exception {
        RecordHeaders headers = new RecordHeaders();
        headers.add("Content-Type", "text/html".getBytes("UTF-8"));
        headers.add("Content-Length", Longs.toByteArray(100L));
        ProducerRecord<String, String> record = new ProducerRecord<>("my-topic", null, "key", "value", headers);

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig());
        producer.send(record);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig());
        consumer.subscribe(Pattern.compile("my-topic"));
        ConsumerRecords<String, String> records;
        while (true) {
            records = consumer.poll(5000);
            if (records.isEmpty()) {
                continue;
            }
            consumer.commitSync();
            break;
        }
        producer.close();

        assertThat(records).hasSize(1).first().satisfies(r -> {
            assertThat(r.topic()).isEqualTo("my-topic");
            assertThat(r.key()).isEqualTo("key");
            assertThat(r.value()).isEqualTo("value");
            assertThat(r.headers()).containsExactly(
                    new RecordHeader("Content-Type", "text/html".getBytes()),
                    new RecordHeader("Content-Length", Longs.toByteArray(100L))
            );
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

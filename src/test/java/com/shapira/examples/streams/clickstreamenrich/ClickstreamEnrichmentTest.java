package com.shapira.examples.streams.clickstreamenrich;

import com.ueokande.rules.rules.LocalKafkaResource;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KeyValue;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

public class ClickstreamEnrichmentTest {

    @Rule
    public LocalKafkaResource server = new LocalKafkaResource("clicks");

    private static List<KeyValue<Integer, String>> expected = Arrays.asList(
            KeyValue.pair(1, "{\"userId\":1,\"userName\":\"Mathias\",\"zipcode\":\"94301\",\"interests\":[\"Surfing\",\"Hiking\"],\"searchTerm\":\"retro wetsuit\",\"page\":\"collections/mens-wetsuits/products/w3-worlds-warmest-wetsuit\"}"),
            KeyValue.pair(2, "{\"userId\":2,\"userName\":\"Anna\",\"zipcode\":\"94303\",\"interests\":[\"Ski\",\"stream processing\"],\"searchTerm\":\"light jacket\",\"page\":\"product/womens-dirt-craft-bike-mountain-biking-jacket\"}"),
            KeyValue.pair(2, "{\"userId\":2,\"userName\":\"Anna\",\"zipcode\":\"94303\",\"interests\":[\"Ski\",\"stream processing\"],\"searchTerm\":\"light jacket\",\"page\":\"/product/womens-ultralight-down-jacket\"}"),
            KeyValue.pair(2, "{\"userId\":2,\"userName\":\"Anna\",\"zipcode\":\"94303\",\"interests\":[\"Ski\",\"stream processing\"],\"searchTerm\":\"carbon ski boots\",\"page\":\"product/salomon-quest-access-custom-heat-ski-boots-womens\"}"),
            KeyValue.pair(2, "{\"userId\":2,\"userName\":\"Anna\",\"zipcode\":\"94303\",\"interests\":[\"Ski\",\"stream processing\"],\"searchTerm\":\"carbon ski boots\",\"page\":\"product/nordica-nxt-75-ski-boots-womens\"}"),
            KeyValue.pair(-1, "{\"userId\":-1,\"userName\":\"\",\"zipcode\":\"\",\"searchTerm\":\"\",\"page\":\"product/osprey-atmos-65-ag-pack\"}")
    );

    @Test
    public void run() throws Exception {
        GenerateData.main(new String[]{});

        ClickstreamEnrichment.main(new String[]{});

        ConsumerRecords<Integer, String> records = receiveClickEnrich();

        assertThat(records).extracting(r -> KeyValue.pair(r.key(), r.value())).containsAll(expected);
    }

    private ConsumerRecords<Integer, String> receiveClickEnrich() {
        try (Consumer<Integer, String> consumer = new KafkaConsumer<>(consumerConfig())) {
            consumer.subscribe(Pattern.compile(Constants.USER_ACTIVITY_TOPIC));
            while (true) {
                ConsumerRecords<Integer, String> records = consumer.poll(5000);
                if (records.isEmpty()) {
                    continue;
                }
                return records;
            }
        }
    }

    private static Properties consumerConfig() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-test");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return props;
    }
}
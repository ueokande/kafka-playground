package com.shapira.examples.streams.stockstats;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.shapira.examples.streams.stockstats.model.TickerWindow;
import com.shapira.examples.streams.stockstats.model.Trade;
import com.shapira.examples.streams.stockstats.model.TradeStats;
import com.shapira.examples.streams.stockstats.serde.JsonSerializer;
import com.ueokande.rules.rules.LocalKafkaResource;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

public class StockStatsExampleTest {

    private static List<ProducerRecord<String, Trade>> source = Arrays.asList(
            new ProducerRecord<>(Constants.STOCK_TOPIC, null, 1500000000000L, "APPLE", new Trade("ACK", "APPLE", 100, 10)),
            new ProducerRecord<>(Constants.STOCK_TOPIC, null, 1500000000000L, "BANANA", new Trade("ACK", "BANANA", 200, 5)),
            new ProducerRecord<>(Constants.STOCK_TOPIC, null, 1500000001000L, "APPLE", new Trade("ACK", "APPLE", 100, 10)),
            new ProducerRecord<>(Constants.STOCK_TOPIC, null, 1500000001000L, "BANANA", new Trade("ACK", "BANANA", 200, 5)),
            new ProducerRecord<>(Constants.STOCK_TOPIC, null, 1500000005000L, "APPLE", new Trade("ACK", "APPLE", 200, 20)),
            new ProducerRecord<>(Constants.STOCK_TOPIC, null, 1500000005000L, "BANANA", new Trade("ACK", "BANANA", 400, 10)),
            new ProducerRecord<>(Constants.STOCK_TOPIC, null, 1500000006000L, "APPLE", new Trade("ACK", "APPLE", 200, 20)),
            new ProducerRecord<>(Constants.STOCK_TOPIC, null, 1500000006000L, "BANANA", new Trade("ACK", "BANANA", 400, 10))
    );

    private static ImmutableMap<String, String> expected = ImmutableMap.<String, String>builder()
            .put("{\"ticker\":\"APPLE\",\"timestamp\":1499999996000}", "{\"type\":\"ACK\",\"ticker\":\"APPLE\",\"countTrades\":1,\"sumPrice\":100.0,\"minPrice\":100.0,\"avgPrice\":100.0}")
            .put("{\"ticker\":\"BANANA\",\"timestamp\":1499999996000}", "{\"type\":\"ACK\",\"ticker\":\"BANANA\",\"countTrades\":1,\"sumPrice\":200.0,\"minPrice\":200.0,\"avgPrice\":200.0}")
            .put("{\"ticker\":\"APPLE\",\"timestamp\":1499999997000}", "{\"type\":\"ACK\",\"ticker\":\"APPLE\",\"countTrades\":2,\"sumPrice\":200.0,\"minPrice\":100.0,\"avgPrice\":100.0}")
            .put("{\"ticker\":\"APPLE\",\"timestamp\":1499999998000}", "{\"type\":\"ACK\",\"ticker\":\"APPLE\",\"countTrades\":2,\"sumPrice\":200.0,\"minPrice\":100.0,\"avgPrice\":100.0}")
            .put("{\"ticker\":\"APPLE\",\"timestamp\":1499999999000}", "{\"type\":\"ACK\",\"ticker\":\"APPLE\",\"countTrades\":2,\"sumPrice\":200.0,\"minPrice\":100.0,\"avgPrice\":100.0}")
            .put("{\"ticker\":\"APPLE\",\"timestamp\":1500000000000}", "{\"type\":\"ACK\",\"ticker\":\"APPLE\",\"countTrades\":2,\"sumPrice\":200.0,\"minPrice\":100.0,\"avgPrice\":100.0}")
            .put("{\"ticker\":\"BANANA\",\"timestamp\":1499999997000}", "{\"type\":\"ACK\",\"ticker\":\"BANANA\",\"countTrades\":2,\"sumPrice\":400.0,\"minPrice\":200.0,\"avgPrice\":200.0}")
            .put("{\"ticker\":\"BANANA\",\"timestamp\":1499999998000}", "{\"type\":\"ACK\",\"ticker\":\"BANANA\",\"countTrades\":2,\"sumPrice\":400.0,\"minPrice\":200.0,\"avgPrice\":200.0}")
            .put("{\"ticker\":\"BANANA\",\"timestamp\":1499999999000}", "{\"type\":\"ACK\",\"ticker\":\"BANANA\",\"countTrades\":2,\"sumPrice\":400.0,\"minPrice\":200.0,\"avgPrice\":200.0}")
            .put("{\"ticker\":\"BANANA\",\"timestamp\":1500000000000}", "{\"type\":\"ACK\",\"ticker\":\"BANANA\",\"countTrades\":2,\"sumPrice\":400.0,\"minPrice\":200.0,\"avgPrice\":200.0}")
            .put("{\"ticker\":\"APPLE\",\"timestamp\":1500000001000}", "{\"type\":\"ACK\",\"ticker\":\"APPLE\",\"countTrades\":2,\"sumPrice\":300.0,\"minPrice\":100.0,\"avgPrice\":150.0}")
            .put("{\"ticker\":\"BANANA\",\"timestamp\":1500000001000}", "{\"type\":\"ACK\",\"ticker\":\"BANANA\",\"countTrades\":2,\"sumPrice\":600.0,\"minPrice\":200.0,\"avgPrice\":300.0}")
            .put("{\"ticker\":\"APPLE\",\"timestamp\":1500000002000}", "{\"type\":\"ACK\",\"ticker\":\"APPLE\",\"countTrades\":2,\"sumPrice\":400.0,\"minPrice\":200.0,\"avgPrice\":200.0}")
            .put("{\"ticker\":\"APPLE\",\"timestamp\":1500000003000}", "{\"type\":\"ACK\",\"ticker\":\"APPLE\",\"countTrades\":2,\"sumPrice\":400.0,\"minPrice\":200.0,\"avgPrice\":200.0}")
            .put("{\"ticker\":\"APPLE\",\"timestamp\":1500000004000}", "{\"type\":\"ACK\",\"ticker\":\"APPLE\",\"countTrades\":2,\"sumPrice\":400.0,\"minPrice\":200.0,\"avgPrice\":200.0}")
            .put("{\"ticker\":\"APPLE\",\"timestamp\":1500000005000}", "{\"type\":\"ACK\",\"ticker\":\"APPLE\",\"countTrades\":2,\"sumPrice\":400.0,\"minPrice\":200.0,\"avgPrice\":200.0}")
            .put("{\"ticker\":\"APPLE\",\"timestamp\":1500000006000}", "{\"type\":\"ACK\",\"ticker\":\"APPLE\",\"countTrades\":1,\"sumPrice\":200.0,\"minPrice\":200.0,\"avgPrice\":200.0}")
            .put("{\"ticker\":\"BANANA\",\"timestamp\":1500000002000}", "{\"type\":\"ACK\",\"ticker\":\"BANANA\",\"countTrades\":2,\"sumPrice\":800.0,\"minPrice\":400.0,\"avgPrice\":400.0}")
            .put("{\"ticker\":\"BANANA\",\"timestamp\":1500000003000}", "{\"type\":\"ACK\",\"ticker\":\"BANANA\",\"countTrades\":2,\"sumPrice\":800.0,\"minPrice\":400.0,\"avgPrice\":400.0}")
            .put("{\"ticker\":\"BANANA\",\"timestamp\":1500000004000}", "{\"type\":\"ACK\",\"ticker\":\"BANANA\",\"countTrades\":2,\"sumPrice\":800.0,\"minPrice\":400.0,\"avgPrice\":400.0}")
            .put("{\"ticker\":\"BANANA\",\"timestamp\":1500000005000}", "{\"type\":\"ACK\",\"ticker\":\"BANANA\",\"countTrades\":2,\"sumPrice\":800.0,\"minPrice\":400.0,\"avgPrice\":400.0}")
            .put("{\"ticker\":\"BANANA\",\"timestamp\":1500000006000}", "{\"type\":\"ACK\",\"ticker\":\"BANANA\",\"countTrades\":1,\"sumPrice\":400.0,\"minPrice\":400.0,\"avgPrice\":400.0}")
            .build();


    @Rule
    public LocalKafkaResource server = new LocalKafkaResource("stockstat");

    @Test
    public void run() throws Exception {

        sendStockStat();

        StockStatsExample.main(new String[]{});

        ConsumerRecords<String, String> records = receiveStockStats();

        for (Map.Entry<String, String> e : expected.entrySet()) {
            assertThat(records).anySatisfy(r -> {
                assertThat(r.key()).isEqualTo(e.getKey());
                assertThat(r.value()).isEqualTo(e.getValue());
            });
        }
    }

    private void sendStockStat() {
        try (Producer<String, Trade> producer = new KafkaProducer<>(producerConfig())) {
            for (ProducerRecord<String, Trade> r : source) {
                producer.send(r);
            }
        }
    }

    private ConsumerRecords<String, String> receiveStockStats() {
        try (Consumer<String, String> consumer = new KafkaConsumer<>(consumerConfig())) {
            consumer.subscribe(Pattern.compile("stockstats-output"));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(5000);
                if (records.isEmpty()) {
                    continue;
                }
                return records;
            }
        }
    }

    private static Properties producerConfig() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        return props;
    }

    private static Properties consumerConfig() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-test");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return props;
    }


    public static class TickerWindowDeserializer implements Deserializer<TickerWindow> {
        private Gson gson = new Gson();

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
        }

        @Override
        public TickerWindow deserialize(String topic, byte[] data) {
            if (data == null) {
                return null;
            }
            return gson.fromJson(new String(data), TickerWindow.class);
        }

        @Override
        public void close() {
        }
    }

    public static class TradeStatsDeserializer implements Deserializer<TradeStats> {
        private Gson gson = new Gson();

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
        }

        @Override
        public TradeStats deserialize(String topic, byte[] data) {
            if (data == null) {
                return null;
            }
            return gson.fromJson(new String(data), TradeStats.class);
        }

        @Override
        public void close() {
        }
    }
}
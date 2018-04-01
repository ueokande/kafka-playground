package com.shapira.examples.streams.stockstats;

import com.shapira.examples.streams.stockstats.model.TickerWindow;
import com.shapira.examples.streams.stockstats.model.Trade;
import com.shapira.examples.streams.stockstats.model.TradeStats;
import com.shapira.examples.streams.stockstats.serde.JsonDeserializer;
import com.shapira.examples.streams.stockstats.serde.JsonSerializer;
import com.shapira.examples.streams.stockstats.serde.WrapperSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.WindowStore;

import java.util.Properties;

/**
 * Input is a stream of trades
 * Output is two streams: One with minimum and avg "ASK" price for every 10 seconds window
 * Another with the top-3 stocks with lowest minimum ask every minute
 */
public class StockStatsExample {

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stockstat");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKER);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, TradeSerde.class.getName());

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        // Note: To re-run the demo, you need to use the offset reset tool:
        // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // work-around for an issue around timing of creating internal topics
        // this was resolved in 0.10.2.0 and above
        // don't use in large production apps - this increases network load
        // props.put(CommonClientConfigs.METADATA_MAX_AGE_CONFIG, 500);


        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Trade> source = builder.stream(Constants.STOCK_TOPIC);

        KStream<TickerWindow, TradeStats> stats = source
                .groupByKey()
                .windowedBy(TimeWindows.of(5000).advanceBy(1000))
                .aggregate(TradeStats::new,
                        (k, v, tradestats) -> tradestats.add(v),
                        Materialized.<String, TradeStats, WindowStore<Bytes, byte[]>>as("serde")
                                .withValueSerde(new TradeStatsSerde()))
                .toStream((key, value) -> new TickerWindow(key.key(), key.window().start()))
                .mapValues((trade) -> trade.computeAvgPrice());

        stats.to(new TickerWindowSerde(), new TradeStatsSerde(), "stockstats-output");


        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        streams.cleanUp();

        streams.start();

        // usually the stream application would be running forever,
        // in this example we just let it run for some time and stop since the input data is finite.
        Thread.sleep(60000L);

        streams.close();

    }

    static public final class TradeSerde extends WrapperSerde<Trade> {
        public TradeSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(Trade.class));
        }
    }

    static public final class TradeStatsSerde extends WrapperSerde<TradeStats> {
        public TradeStatsSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(TradeStats.class));
        }
    }

    static public final class TickerWindowSerde extends WrapperSerde<TickerWindow> {
        public TickerWindowSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(TickerWindow.class));
        }
    }
}

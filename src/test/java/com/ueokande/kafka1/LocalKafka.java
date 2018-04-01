package com.ueokande.kafka1;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

import java.io.File;
import java.util.Properties;

public class LocalKafka {
    private final KafkaServerStartable server;

    LocalKafka(Properties props) {
        server = new KafkaServerStartable(new KafkaConfig(props));
    }

    public void start() {
        server.startup();
    }

    public void shutdown() {
        server.shutdown();
    }

    static Properties properties(File dataDir, int zkPort) {
        Properties props = new Properties();
        props.put("broker.id", 0);
        props.put("num.network.threads", 3);
        props.put("num.io.threads", 8);
        props.put("socket.send.buffer.bytes", 102400);
        props.put("socket.receive.buffer.bytes", 102400);
        props.put("socket.request.max.bytes", 104857600);
        props.put("log.dir", dataDir.getAbsolutePath());
        props.put("num.partitions", 1);
        props.put("num.recovery.threads.per.data.dir", 1);
        props.put("offsets.topic.replication.factor", (short) 1);
        props.put("transaction.state.log.replication.factor", (short) 1);
        props.put("transaction.state.log.min.isr", 1);
        props.put("log.retention.hours", 168);
        props.put("log.segment.bytes", 1073741824);
        props.put("log.retention.check.interval.ms", 300000);
        props.put("zookeeper.connect", "localhost:" + zkPort + "/kafka");
        props.put("zookeeper.connection.timeout.ms", 6000);
        props.put("group.initial.rebalance.delay.ms", 0);
        return props;
    }
}

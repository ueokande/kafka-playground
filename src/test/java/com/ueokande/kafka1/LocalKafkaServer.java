package com.ueokande.kafka1;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import java.io.File;
import java.io.IOException;

public class LocalKafkaServer {
    private static final int ZOOKEEPER_PORT = 2181;

    private final LocalZookeeper zk;
    private static LocalKafka kafka;

    public LocalKafkaServer(File zkDataDir, File kafkaDataDir) throws IOException, QuorumPeerConfig.ConfigException {
        zk = new LocalZookeeper(LocalZookeeper.properties(zkDataDir, ZOOKEEPER_PORT));
        kafka = new LocalKafka(LocalKafka.properties(kafkaDataDir, ZOOKEEPER_PORT));
    }

    public void start() {
        new Thread(() -> {
            try {
                zk.start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
        kafka.start();
    }

    public void close() {
        kafka.shutdown();
    }
}

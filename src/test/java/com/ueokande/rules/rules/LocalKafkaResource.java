package com.ueokande.rules.rules;

import org.apache.commons.io.FileUtils;
import org.junit.rules.ExternalResource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

public class LocalKafkaResource extends ExternalResource {
    private static final int ZOOKEEPER_PORT = 2181;

    private String cleanDir;
    private Path zkDataDir;
    private Path kafkaDataDir;
    private LocalZookeeper zk;
    private LocalKafka kafka;

    public LocalKafkaResource() {
    }

    public LocalKafkaResource(String cleanDir) {
        this.cleanDir = cleanDir;
    }

    @Override
    protected void before() throws Throwable {
        if (Objects.nonNull(cleanDir)) {
            FileUtils.forceDeleteOnExit(new File(cleanDir));
        }

        zkDataDir = Files.createTempDirectory("zookeeper");
        kafkaDataDir = Files.createTempDirectory("kafka");

        zk = new LocalZookeeper(LocalZookeeper.properties(zkDataDir.toFile(), ZOOKEEPER_PORT));
        kafka = new LocalKafka(LocalKafka.properties(kafkaDataDir.toFile(), ZOOKEEPER_PORT));

        new Thread(() -> {
            try {
                zk.start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
        kafka.start();
    }

    @Override
    protected void after() {
        kafka.shutdown();
        try {
            FileUtils.forceDelete(zkDataDir.toFile());
            FileUtils.forceDelete(kafkaDataDir.toFile());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

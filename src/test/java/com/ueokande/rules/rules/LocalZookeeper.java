package com.ueokande.rules.rules;

import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

class LocalZookeeper {
    private final ServerConfig zkConfig;

    LocalZookeeper(Properties props) throws IOException, QuorumPeerConfig.ConfigException {
        QuorumPeerConfig quorumConfig = new QuorumPeerConfig();
        quorumConfig.parseProperties(props);
        zkConfig = new ServerConfig();
        zkConfig.readFrom(quorumConfig);
    }

    public void start() throws IOException {
        ZooKeeperServerMain zk = new ZooKeeperServerMain();
        zk.runFromConfig(zkConfig);
    }


    static Properties properties(File dataDir, int clientPort) {
        Properties props = new Properties();
        props.put("dataDir", dataDir.getAbsolutePath());
        props.put("clientPort", clientPort);
        return props;
    }
}

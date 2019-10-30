/*
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at
 http://www.apache.org/licenses/LICENSE-2.0
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 limitations under the License.
 */

package org.facboy;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.util.Properties;
import kafka.server.KafkaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * From <a href="https://github.com/apache/flume/blob/trunk/flume-ng-sinks/flume-ng-kafka-sink/src/test/java/org/apache/flume/sink/kafka/util/TestUtil.java">
 *     https://github.com/apache/flume/blob/trunk/flume-ng-sinks/flume-ng-kafka-sink/src/test/java/org/apache/flume/sink/kafka/util/TestUtil.java</a>
 */
public class KafkaTestServer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaTestServer.class);

    private final File dataDirectory;

    private final String hostname = "localhost";
    private KafkaLocal kafkaServer;
    private ZooKeeperLocal zooKeeperLocal;
    private int kafkaLocalPort;
    private int zkLocalPort;

    public KafkaTestServer(File dataDirectory) {
        this.dataDirectory = dataDirectory;
    }

    public void start() throws IOException {
        Properties kafkaProperties = new Properties();
        Properties zkProperties = new Properties();

        logger.info("Starting kafka server.");

        //load properties
        try (InputStream is = KafkaTestServer.class.getResourceAsStream("zookeeper.properties")) {
            zkProperties.load(is);
        }

        //start local Zookeeper
        zkLocalPort = getNextPort();
        // override the Zookeeper client port with the generated one.
        zkProperties.setProperty("clientPort", Integer.toString(zkLocalPort));
        zkProperties.setProperty("dataDir", dataDirectory.getAbsolutePath());
        zooKeeperLocal = new ZooKeeperLocal(zkProperties);
        zooKeeperLocal.start();

        logger.info("ZooKeeper instance is successfully started on port "
                + zkLocalPort);

        try (InputStream is = KafkaTestServer.class.getResourceAsStream("kafka-server.properties")) {
            kafkaProperties.load(is);
        }
        // override the Zookeeper url.
        kafkaProperties.setProperty(KafkaConfig.ZkConnectProp(), getZkUrl());
        kafkaProperties.setProperty(KafkaConfig.LogDirsProp(), new File(dataDirectory, "kafka-logs").getAbsolutePath());
        kafkaLocalPort = getNextPort();
        // override the Kafka server port
        kafkaProperties.setProperty(KafkaConfig.PortProp(), Integer.toString(kafkaLocalPort));
        kafkaServer = new KafkaLocal(kafkaProperties);
        kafkaServer.start();
        logger.info("Kafka Server is successfully started on port " + kafkaLocalPort);
    }

    public void stop() {
        try {
            if (kafkaServer != null) {
                kafkaServer.stop();
            }
        } finally {
            if (zooKeeperLocal != null) {
                zooKeeperLocal.close();
            }
        }
    }

    public String getZkUrl() {
        return hostname + ":" + zkLocalPort;
    }

    public String getKafkaUrl() {
        return hostname + ":" + kafkaLocalPort;
    }

    private synchronized int getNextPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}

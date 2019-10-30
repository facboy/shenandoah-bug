/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * limitations under the License.
 */

package org.facboy;

import com.google.common.base.Throwables;
import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.util.Properties;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * From <a href="https://github.com/apache/flume/blob/trunk/flume-ng-sinks/flume-ng-kafka-sink/src/test/java/org/apache/flume/sink/kafka/util/TestUtil.java">
 * https://github.com/apache/flume/blob/trunk/flume-ng-sinks/flume-ng-kafka-sink/src/test/java/org/apache/flume/sink/kafka/util/TestUtil.java</a>
 */
class ZooKeeperLocal implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ZooKeeperLocal.class);

    private static final MethodHandle GET_CNXN_FACTORY;

    static {
        try {
            Method method = ZooKeeperServerMain.class.getDeclaredMethod("getCnxnFactory");
            method.setAccessible(true);

            GET_CNXN_FACTORY = MethodHandles.lookup().unreflect(method);
        } catch (NoSuchMethodException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final ZooKeeperServerMain zooKeeperServer;
    private final Properties zkProperties;

    public ZooKeeperLocal(Properties zkProperties) {
        this.zkProperties = zkProperties;
        this.zooKeeperServer = new ZooKeeperServerMain();
    }

    public void start() {
        QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();
        try {
            quorumConfiguration.parseProperties(zkProperties);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        final ServerConfig configuration = new ServerConfig();
        configuration.readFrom(quorumConfiguration);
        new Thread(() -> {
            try {
                zooKeeperServer.runFromConfig(configuration);
            } catch (IOException e) {
                logger.error("Zookeeper startup failed.", e);
            }
        }).start();
    }

    @Override
    public void close() {
        try {
            ServerCnxnFactory serverCnxnFactory = (ServerCnxnFactory) GET_CNXN_FACTORY.invoke(zooKeeperServer);
            serverCnxnFactory.shutdown();
        } catch (Throwable throwable) {
            Throwables.throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
    }
}

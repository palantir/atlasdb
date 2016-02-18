/**
 * Copyright 2015 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.cassandra.jmx;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.cassandra.CassandraJmxCompactionConfig;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;

public class CassandraJmxCompactionManagers {
    private CassandraJmxCompactionManagers(){
        // private constructor to prevent subclassing
    }

    /**
     * Running compaction against C* without SSL enabled experiences some hanging tcp connection
     * during compaction processes. So System.setProperty("sun.rmi.transport.tcp.responseTimeout",
     * String.valueOf(jmxRmiTimeoutMillis)); will enforce the tcp connection timeout in case it
     * happens.
     *
     * @param jmxConfig
     */
    private static void setJmxSslProperty(CassandraJmxCompactionConfig jmxConfig) {
        long rmiTimeoutMillis = jmxConfig.rmiTimeoutMillis();
        // NOTE: RMI timeout to avoid hanging tcp connection
        System.setProperty("sun.rmi.transport.tcp.responseTimeout", String.valueOf(rmiTimeoutMillis));

        String keyStoreFile = jmxConfig.keystore();
        String keyStorePassword = jmxConfig.keystorePassword();
        String trustStoreFile = jmxConfig.truststore();
        String trustStorePassword = jmxConfig.truststorePassword();
        Preconditions.checkState((new File(keyStoreFile)).exists(), "file:" + keyStoreFile + " does not exist!");
        Preconditions.checkState((new File(trustStoreFile)).exists(), "file:" + trustStoreFile + " does not exist!");
        System.setProperty("javax.net.ssl.keyStore", keyStoreFile);
        System.setProperty("javax.net.ssl.keyStorePassword", keyStorePassword);
        System.setProperty("javax.net.ssl.trustStore", trustStoreFile);
        System.setProperty("javax.net.ssl.trustStorePassword", trustStorePassword);
    }


    /**
     * return a empty set if not client can be created
     * @param config
     * @return
     */
    public static Set<CassandraJmxCompactionClient> createCompactionClients(CassandraKeyValueServiceConfig config) {
        if (!config.jmx().isPresent()) {
            return Collections.emptySet();
        }

        CassandraJmxCompactionConfig jmxConfig = config.jmx().get();
        // need to set the property before creating the JMX compaction client
        setJmxSslProperty(jmxConfig);

        Set<CassandraJmxCompactionClient> clients = Sets.newHashSet();
        Set<InetSocketAddress> thriftEndPoints = config.servers();
        Preconditions.checkState(!thriftEndPoints.isEmpty(), "address_list should not be empty.");

        // jmxEndPoints are using different ports specified in address_list
        int jmxPort = jmxConfig.port();
        for (InetSocketAddress addr : thriftEndPoints) {
            Optional<CassandraJmxCompactionClient> client = new CassandraJmxCompactionClient.Builder(addr.getHostString(), jmxPort)
                        .username(jmxConfig.username())
                        .password(jmxConfig.password())
                        .build();
            if (client.isPresent()) {
                clients.add(client.get());
            }
        }

        return clients;
    }
}

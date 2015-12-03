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
package com.palantir.atlasdb.keyvalue.cassandra;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import javax.annotation.concurrent.GuardedBy;

import org.apache.cassandra.thrift.Cassandra.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.net.InetAddresses;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.pooling.ForwardingPoolingContainer;
import com.palantir.common.pooling.PoolingContainer;

public class ManyClientPoolingContainer extends ForwardingPoolingContainer<Client>
        implements ManyHostPoolingContainer<Client> {
    private static final Logger log = LoggerFactory.getLogger(ManyClientPoolingContainer.class);
    volatile ImmutableList<PoolingContainer<Client>> containers = ImmutableList.of();
    @GuardedBy("this")
    final Map<String, PoolingContainer<Client>> containerMap = Maps.newHashMap();
    @GuardedBy("this")
    boolean isShutdown = false;
    boolean safetyDisabled = false;
    private final Random random = new Random();

    public static ManyClientPoolingContainer create(CassandraKeyValueServiceConfig config) {
        ManyClientPoolingContainer ret = new ManyClientPoolingContainer();
        ret.setNewHosts(config);
        return ret;
    }

    public synchronized void setNewHosts(CassandraKeyValueServiceConfig config) {
        String keyspace = config.keyspace();
        int port = config.port();
        int poolSize = config.poolSize();
        boolean isSsl = config.ssl();
        int socketTimeoutMillis = config.socketTimeoutMillis();
        int socketQueryTimeoutMillis = config.socketQueryTimeoutMillis();

        Set<String> toRemove = Sets.difference(containerMap.keySet(), config.servers()).immutableCopy();
        Set<String> toAdd = Sets.difference(config.servers(), containerMap.keySet()).immutableCopy();
        for (String host : toRemove) {
            PoolingContainer<Client> pool = containerMap.remove(host);
            Preconditions.checkNotNull(pool);
            log.warn("Shutting down client pool for {}", host);
            pool.shutdownPooling();
        }

        if (!toAdd.isEmpty()) {
            CassandraVerifier.sanityCheckRingConsistency(
                    Sets.union(containerMap.keySet(), toAdd),
                    port,
                    keyspace,
                    isSsl,
                    safetyDisabled,
                    socketTimeoutMillis,
                    socketQueryTimeoutMillis);
        }

        for (String host : toAdd) {
            if (isShutdown) {
                log.warn("client Pool is shutdown, cannot add hosts:{}", toAdd);
                break;
            }
            PoolingContainer<Client> newPool = createPool(host, port, keyspace, poolSize, isSsl, socketTimeoutMillis, socketQueryTimeoutMillis);
            containerMap.put(host, newPool);
            log.info("Created pool {} for host {}", newPool, host);
        }
        containers = ImmutableList.copyOf(containerMap.values());
    }

    private PoolingContainer<Client> createPool(String host, int port, String keyspace, int poolSize, boolean isSsl, int socketTimeoutMillis, int socketQueryTimeoutMillis) {
        return new CassandraClientPoolingContainer.Builder(host, port)
            .poolSize(poolSize)
            .keyspace(keyspace)
            .isSsl(isSsl)
            .socketTimeout(socketTimeoutMillis)
            .socketQueryTimeout(socketQueryTimeoutMillis)
            .build();
    }

    public synchronized List<String> getCurrentHosts() {
        return ImmutableList.copyOf(containerMap.keySet());
    }

    @Override
    public synchronized void shutdownPooling() {
        isShutdown = true;
        for (PoolingContainer<Client> pool : containers) {
            pool.shutdownPooling();
        }
    }

    @Override
    protected PoolingContainer<Client> delegate() {
        List<PoolingContainer<Client>> list = containers;
        return list.get(random.nextInt(list.size()));
    }

    @Override
    public <V, K extends Exception> V runWithPooledResourceOnHost(InetAddress host,
                                                                  FunctionCheckedException<Client, V, K> f) throws K {
        return delegateForHost(host).runWithPooledResource(f);
    }

    @Override
    public <V> V runWithPooledResourceOnHost(InetAddress host, Function<Client, V> f) {
        return delegateForHost(host).runWithPooledResource(f);
    }

    private PoolingContainer<Client> delegateForHost(InetAddress host) {
        for (Map.Entry<String, PoolingContainer<Client>> entry : containerMap.entrySet()) {
            if (InetAddresses.forString(entry.getKey()).equals(host)) {
                return entry.getValue();
            }
        }
        log.warn("Unrecognized host {}, falling back to a randomly chosen client", host);
        return delegate();
    }

}
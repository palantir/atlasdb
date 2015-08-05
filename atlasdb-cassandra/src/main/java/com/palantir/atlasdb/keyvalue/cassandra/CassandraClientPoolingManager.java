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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.pooling.PoolingContainer;

public class CassandraClientPoolingManager {
    private static final Logger log = LoggerFactory.getLogger(CassandraClientPoolingManager.class);

    private final ManyClientPoolingContainer containerPoolToUpdate;
    private final PoolingContainer<Client> clientPool;
    private final int port;
    private final boolean isSsl;
    private final int poolSize;
    private final String keyspace;
    private boolean safetyDisabled;
    private boolean autoRefreshNodes;

    private final ScheduledExecutorService hostRefreshExecutor = PTExecutors.newScheduledThreadPool(1);

    public CassandraClientPoolingManager(ManyClientPoolingContainer containerPoolToUpdate,
                                         PoolingContainer<Client> clientPool,
                                         int port,
                                         boolean isSsl,
                                         int poolSize,
                                         String keyspace,
                                         boolean safetyDisabled,
                                         boolean autoRefreshNodes) {
        this.containerPoolToUpdate = containerPoolToUpdate;
        this.clientPool = clientPool;
        this.port = port;
        this.isSsl = isSsl;
        this.poolSize = poolSize;
        this.keyspace = keyspace;
        this.safetyDisabled = safetyDisabled;
        this.autoRefreshNodes = autoRefreshNodes;
    }

    public void submitHostRefreshTask() {
        hostRefreshExecutor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    if (hostsAutoRefresh()) {
                        setHostsToCurrentHostNames();
                    }
                } catch (Throwable t) {
                    log.error("Failed to get current cluster info from cassandra", t);
                }
            }
        }, CassandraConstants.SECONDS_BETWEEN_GETTING_HOST_LIST, CassandraConstants.SECONDS_BETWEEN_GETTING_HOST_LIST, TimeUnit.SECONDS);
    }

    public void setHostsToCurrentHostNames() throws TException {
        clientPool.runWithPooledResource(new FunctionCheckedException<Cassandra.Client, Void, TException>() {
            @Override
            public Void apply(Client client) throws TException {
                setHostsToCurrentHostNames(getCurrentHostNamesFromServer(client));
                return null;
            }

        });
    }

    public void setHostsToCurrentHostNames(Set<String> currentHosts) {
        containerPoolToUpdate.setNewHosts(currentHosts, port, poolSize, keyspace, isSsl, safetyDisabled);
    }

    public Set<String> getCurrentHostNamesFromServer(Client c) throws TException {
        Map<String, String> tokenMap;
        try {
            tokenMap = c.describe_token_map();
        } catch (InvalidRequestException e) {
            throw Throwables.throwUncheckedException(e);
        }
        return ImmutableSet.copyOf(tokenMap.values());
    }

    public boolean hostsAutoRefresh() {
        return autoRefreshNodes;
    }

}

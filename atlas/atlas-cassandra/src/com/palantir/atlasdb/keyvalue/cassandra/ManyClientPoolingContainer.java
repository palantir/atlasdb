// Copyright 2015 Palantir Technologies
//
// Licensed under the BSD-3 License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://opensource.org/licenses/BSD-3-Clause
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.keyvalue.cassandra;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import javax.annotation.concurrent.GuardedBy;

import org.apache.cassandra.thrift.Cassandra.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.palantir.common.pooling.ForwardingPoolingContainer;
import com.palantir.common.pooling.PoolingContainer;

public class ManyClientPoolingContainer extends ForwardingPoolingContainer<Client> {

    private static final Logger log = LoggerFactory.getLogger(ManyClientPoolingContainer.class);

    volatile ImmutableList<PoolingContainer<Client>> containers = ImmutableList.of();
    @GuardedBy("this") final Map<String, PoolingContainer<Client>> containerMap = Maps.newHashMap();
    @GuardedBy("this") boolean isShutdown = false;
    boolean safetyDisabled = false;
    private final Random random = new Random();

    public static ManyClientPoolingContainer create(Set<String> initialHosts, int port, int poolSize, String keyspace, boolean isSsl, boolean safetyDisabled) {
        ManyClientPoolingContainer ret = new ManyClientPoolingContainer();
        ret.setNewHosts(initialHosts, port, poolSize, keyspace, isSsl, safetyDisabled);
        return ret;
    }

    public synchronized void setNewHosts(Set<String> hosts, int port, int poolSize, String keyspace, boolean isSsl, boolean safetyDisabled) {
        Set<String> toRemove = Sets.difference(containerMap.keySet(), hosts).immutableCopy();
        Set<String> toAdd = Sets.difference(hosts, containerMap.keySet()).immutableCopy();
        for (String host : toRemove) {
            PoolingContainer<Client> pool = containerMap.remove(host);
            Preconditions.checkNotNull(pool);
            pool.shutdownPooling();
        }

        if (!toAdd.isEmpty()) {
            CassandraVerifier.sanityCheckRingConsistency(Sets.union(containerMap.keySet(), toAdd), port, keyspace, isSsl, safetyDisabled);
        }
        for (String host : toAdd) {
            CassandraClientPoolingContainer newPool = new CassandraClientPoolingContainer(host, port, poolSize, keyspace, isSsl);
            containerMap.put(host, newPool);
            log.info("Created pool {} for host {}", newPool, host);
            if (isShutdown) {
                newPool.shutdownPooling();
            }
        }
        containers = ImmutableList.copyOf(containerMap.values());
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

}

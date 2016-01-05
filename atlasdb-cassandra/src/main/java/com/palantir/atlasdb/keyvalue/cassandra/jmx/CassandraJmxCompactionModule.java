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

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;

public class CassandraJmxCompactionModule {
    private static final Logger log = LoggerFactory.getLogger(CassandraJmxCompactionModule.class);

    public Optional<CassandraJmxCompactionManager> createCompactionManager(CassandraKeyValueServiceConfigManager configManager) {
        final CassandraKeyValueServiceConfig config = configManager.getConfig();
        Set<CassandraJmxCompactionClient> clients = CassandraJmxCompactionManagers.createCompactionClients(config);

        if (clients.isEmpty()) {
            log.warn("No compaction client can be found.");
            return Optional.absent();
        }

        ExecutorService exec = Executors.newFixedThreadPool(
                clients.size(),
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Cassandra-Jmx-Compaction-ThreadPool-%d").build());
        return Optional.of(CassandraJmxCompactionManager.newInstance(clients, exec));
    }
}

/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.workload.migration.jmx;

import com.google.common.collect.Iterables;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.cassandra.service.StorageProxyMBean;
import org.apache.cassandra.service.StorageServiceMBean;

public class JmxCassandraStateManager implements CassandraStateManager {
    private static final SafeLogger log = SafeLoggerFactory.get(JmxCassandraStateManager.class);

    private final Supplier<CassandraJmxConnector> connectorFactory;

    public JmxCassandraStateManager(Supplier<CassandraJmxConnector> connectorFactory) {
        this.connectorFactory = connectorFactory;
    }

    @Override // If we actually keep this, move from stringly typed code
    public void forceRebuild(String datacenter, String keyspace) {
        runConsumerWithSsProxy(proxy -> {
            log.info("Rebuilding keyspace from datacenter");
            proxy.rebuild(datacenter, keyspace);
        });
    }

    @Override
    public Optional<String> getConsensusSchemaVersionFromNode() {
        Map<String, List<String>> schemaVersions = runFunctionWithStorageProxy(StorageProxyMBean::getSchemaVersions);
        Set<String> uniqueSchemaVersions =
                schemaVersions.values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
        if (uniqueSchemaVersions.size() == 1) {
            return Optional.of(Iterables.getOnlyElement(uniqueSchemaVersions));
        } else {
            return Optional.empty();
        }
        // Consider using CassandraKeyValueServices.waitForSchemaVersion
    }

    @Override
    public void enablingClientInterfaces() {
        runConsumerWithSsProxy(StorageServiceMBean::persistentEnableClientInterfaces);
    }

    private <T> T runFunctionWithStorageProxy(Function<StorageProxyMBean, T> function) {
        try (CassandraJmxConnector connector = connectorFactory.get()) {
            return function.apply(getStorageProxy(connector));
        }
    }

    private void runConsumerWithSsProxy(Consumer<StorageServiceMBean> consumer) {
        try (CassandraJmxConnector connector = connectorFactory.get()) {
            consumer.accept(getStorageService(connector));
        }
    }

    private StorageServiceMBean getStorageService(CassandraJmxConnector connector) {
        return connector.getMBeanProxy("org.apache.cassandra.db:type=StorageService", StorageServiceMBean.class);
    }

    private StorageProxyMBean getStorageProxy(CassandraJmxConnector connector) {
        return connector.getMBeanProxy("org.apache.cassandra.db:type=StorageProxy", StorageProxyMBean.class);
    }
}

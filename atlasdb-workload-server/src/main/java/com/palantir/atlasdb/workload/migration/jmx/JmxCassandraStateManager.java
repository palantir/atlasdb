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
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.cassandra.service.StorageProxyMBean;
import org.apache.cassandra.service.StorageServiceMBean;

public class JmxCassandraStateManager implements CassandraStateManager {
    private static final SafeLogger log = SafeLoggerFactory.get(JmxCassandraStateManager.class);
    private static final Duration REBUILD_NODE_VERIFICATION_INTERVAL = Duration.ofSeconds(20);

    private final Supplier<CassandraJmxConnector> connectorFactory;

    public JmxCassandraStateManager(Supplier<CassandraJmxConnector> connectorFactory) {
        this.connectorFactory = connectorFactory;
    }

    @Override // If we actually keep this, move from stringly typed code
    public void forceRebuild(String sourceDatacenter, Set<String> keyspaces) {
        // For now, we're relying on this being a blocking call
        keyspaces.forEach(keyspace -> {
            runConsumerWithSsProxy(proxy -> {
                log.info(
                        "Rebuilding keyspace {} from source DC {}",
                        SafeArg.of("keyspace", keyspace),
                        SafeArg.of("sourceDatacenter", sourceDatacenter));
                proxy.rebuild(sourceDatacenter, keyspace);
                log.info(
                        "Finished rebuilding keyspace {} from source DC {}",
                        SafeArg.of("keyspace", keyspace),
                        SafeArg.of("sourceDatacenter", sourceDatacenter));
                // TODO: Test that the keyspace is rebuilt
            });
        });
    }

    @Override
    public Optional<String> getConsensusSchemaVersionFromNode() {
        Map<String, List<String>> schemaVersions = runFunctionWithStorageProxy(StorageProxyMBean::getSchemaVersions);
        log.info("Schema versions {}", SafeArg.of("schemaVersions", schemaVersions));
        Set<String> uniqueSchemaVersions = schemaVersions.keySet();
        if (uniqueSchemaVersions.size() == 1) {
            return Optional.of(Iterables.getOnlyElement(uniqueSchemaVersions));
        } else {
            return Optional.empty();
        }
        // Consider using CassandraKeyValueServices.waitForSchemaVersion
    }

    @Override
    public void enableClientInterfaces() {
        runConsumerWithSsProxy(StorageServiceMBean::persistentEnableClientInterfaces);
    }

    @Override
    public InterfaceStates getInterfaceState() {
        return runFunctionWithSsProxy(proxy -> {
            boolean gossipRunning = proxy.isGossipRunning();
            log.debug("State of isGossipRunning {}", SafeArg.of("isGossispRunning", gossipRunning));

            boolean rpcServerRunning = proxy.isRPCServerRunning();
            log.debug("State of isRpcServerRunning {}", SafeArg.of("isRpcServerRunning", rpcServerRunning));

            boolean nativeTransportRunning = proxy.isNativeTransportRunning();
            log.debug(
                    "State of isNativeTransportRunning {}",
                    SafeArg.of("isNativeTransportRunning", nativeTransportRunning));

            return InterfaceStates.builder()
                    .gossipIsRunning(gossipRunning)
                    .nativeTransportIsRunning(nativeTransportRunning)
                    .rpcServerIsRunning(rpcServerRunning)
                    .build();
        });
    }

    @Override
    public Set<String> getRebuiltKeyspaces(String sourceDatacenter) {
        // C* seems to have a bug with determining whether keyspaces are missing ranges or not during startup.
        verifyNodeUpNormal();
        Duration initialUptime = getJvmUptime();
        Set<String> completeKeyspaces = getKeyspacesWithAllRangesAvailable(sourceDatacenter);
        waitForVerificationInterval();

        verifySameRangesAvailable(completeKeyspaces, getKeyspacesWithAllRangesAvailable(sourceDatacenter));
        waitForVerificationInterval();

        verifySameRangesAvailable(completeKeyspaces, getKeyspacesWithAllRangesAvailable(sourceDatacenter));
        Duration endingUptime = getJvmUptime();

        Preconditions.checkState(
                initialUptime.minus(endingUptime).isNegative(),
                "Cassandra JVM was not up during entirety of rebuild verification. Aborting this rebuild iteration: {}"
                        + " {}",
                SafeArg.of("initialUptimeMillis", initialUptime.toMillis()),
                SafeArg.of("endingUptimeMillis", endingUptime.toMillis()));
        verifyNodeUpNormal();

        return completeKeyspaces;
    }

    private void verifySameRangesAvailable(Set<String> initial, Set<String> current) {
        if (initial.equals(current)) {
            return;
        }
        throw new SafeRuntimeException(
                "Node returned inconsistent keyspaces with all ranges available {} {}",
                SafeArg.of("initial", initial),
                SafeArg.of("current", current));
    }

    private void waitForVerificationInterval() {
        try {
            Thread.sleep(REBUILD_NODE_VERIFICATION_INTERVAL.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private Duration getJvmUptime() {
        return Duration.ofSeconds(1); // TODO;
    }

    private void verifyNodeUpNormal() {
        String operationMode = runFunctionWithSsProxy(StorageServiceMBean::getOperationMode);
        if (operationMode.equalsIgnoreCase("NORMAL")) {
            return;
        }
        throw new SafeRuntimeException(
                "Node is no longer UN during rebuild task execution", SafeArg.of("state", operationMode));
    }

    private Set<String> getKeyspacesWithAllRangesAvailable(String sourceDatacenter) {
        return runFunctionWithSsProxy(proxy -> proxy.getKeyspacesWithAllRangesAvailable(sourceDatacenter));
    }

    private <T> T runFunctionWithStorageProxy(Function<StorageProxyMBean, T> function) {
        try (CassandraJmxConnector connector = connectorFactory.get()) {
            return function.apply(getStorageProxy(connector));
        }
    }

    private <T> T runFunctionWithSsProxy(Function<StorageServiceMBean, T> function) {
        try (CassandraJmxConnector connector = connectorFactory.get()) {
            return function.apply(getStorageService(connector));
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

/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.debug;

import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.AtlasDbRuntimeConfig;
import com.palantir.atlasdb.config.ImmutableTimeLockClientConfig;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.config.ServerListConfigs;
import com.palantir.atlasdb.config.TimeLockClientConfig;
import com.palantir.atlasdb.debug.ClientLockDiagnosticCollector.ClientLockDiagnosticDigest;
import com.palantir.atlasdb.debug.FullDiagnosticDigest.LockDigest;
import com.palantir.atlasdb.debug.FullDiagnosticDigest.TransactionDigest;
import com.palantir.atlasdb.factory.ServiceCreator;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.persist.Persistable;
import com.palantir.common.streams.KeyedStream;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.lock.LockDescriptor;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import com.palantir.util.OptionalResolver;

/**
 * TODO(fdesouza): Remove this once PDS-95791 is resolved.
 * @deprecated Remove this once PDS-95791 is resolved.
 */
@Deprecated
public class TransactionPostMortemRunner {

    private final String timelockNamespace;
    private final ClientLockDiagnosticCollector clientLockDiagnosticCollector;
    private final WritesDigestEmitter writesDigestEmitter;
    private final LockDiagnosticInfoService timelockDiagnosticService;

    public TransactionPostMortemRunner(
            TransactionManager transactionManager,
            TableReference tableReference,
            AtlasDbConfig install,
            Supplier<AtlasDbRuntimeConfig> runtime,
            ClientLockDiagnosticCollector clientLockDiagnosticCollector) {
        this.timelockNamespace = timelockNamespace(install);
        this.clientLockDiagnosticCollector = clientLockDiagnosticCollector;
        this.timelockDiagnosticService = createRpcClient(install, runtime);
        this.writesDigestEmitter = new WritesDigestEmitter(transactionManager, tableReference);
    }

    public <T> FullDiagnosticDigest<T> conductPostMortem(
            Persistable row,
            byte[] columnName,
            Function<Value, T> deserializer) {
        WritesDigest<T> digest = writesDigestEmitter.getDigest(row, columnName, deserializer);
        Map<Long, ClientLockDiagnosticDigest> snapshot = clientLockDiagnosticCollector.getSnapshot();
        Optional<LockDiagnosticInfo> lockDiagnosticInfo =
                timelockDiagnosticService.getEnhancedLockDiagnosticInfo(timelockNamespace, requestIds(snapshot));

        Set<UUID> lockRequestIdsEvictedMidLockRequest = lockDiagnosticInfo
                .map(LockDiagnosticInfo::requestIdsEvictedMidLockRequest)
                .orElseGet(ImmutableSet::of);

        Set<TransactionDigest<T>> transactionDigests = digest.completedOrAbortedTransactions().keySet().stream()
                .map(startTimestamp -> transactionDigest(
                        startTimestamp,
                        digest,
                        lockDiagnosticInfo,
                        snapshot.getOrDefault(startTimestamp, ClientLockDiagnosticDigest.missingEntry())))
                .collect(Collectors.toSet());

        return ImmutableFullDiagnosticDigest.<T>builder()
                .rawData(ImmutableRawData.of(digest, lockDiagnosticInfo, snapshot))
                .addAllInProgressTransactions(digest.inProgressTransactions())
                .lockRequestIdsEvictedMidLockRequest(lockRequestIdsEvictedMidLockRequest)
                .transactionDigests(transactionDigests)
                .build();
    }

    private static Set<UUID> requestIds(Map<Long, ClientLockDiagnosticDigest> clientDigests) {
        return clientDigests.values().stream()
                .map(TransactionPostMortemRunner::requestIds)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
    }

    private static Set<UUID> requestIds(ClientLockDiagnosticDigest clientDigest) {
        return ImmutableSet.<UUID>builder()
                .addAll(clientDigest.lockRequests().keySet())
                .add(clientDigest.immutableTimestampRequestId())
                .build();
    }

    private static <T> TransactionDigest<T> transactionDigest(
            long startTimestamp,
            WritesDigest<T> writesDigest,
            Optional<LockDiagnosticInfo> timelockLockInfo,
            ClientLockDiagnosticDigest clientLockDigest) {
        Map<UUID, Set<LockDescriptor>> lockRequests = ImmutableMap.<UUID, Set<LockDescriptor>>builder()
                .putAll(clientLockDigest.lockRequests())
                .put(clientLockDigest.immutableTimestampRequestId(), ImmutableSet.of())
                .build();
        Map<UUID, LockDigest> lockDigests = KeyedStream.stream(lockRequests)
                .map((requestId, descriptors) ->
                        lockDigest(descriptors, timelockLockInfo.map(info -> lockState(requestId, info))))
                .collectToMap();

        return ImmutableTransactionDigest.<T>builder()
                .startTimestamp(startTimestamp)
                .commitTimestamp(writesDigest.completedOrAbortedTransactions().get(startTimestamp))
                .value(writesDigest.allWrittenValuesDeserialized().get(startTimestamp))
                .immutableTimestamp(clientLockDigest.immutableTimestamp())
                .immutableTimestampLockRequestId(clientLockDigest.immutableTimestampRequestId())
                .locks(lockDigests)
                .build();
    }

    private static Map<LockState, Instant> lockState(UUID requestId, LockDiagnosticInfo diagnosticInfo) {
        return diagnosticInfo.lockInfos().get(requestId).lockStates();
    }

    private static LockDigest lockDigest(
            Set<LockDescriptor> lockDescriptors,
            Optional<Map<LockState, Instant>> maybeLockStates) {
        Map<LockState, Instant> lockStates = maybeLockStates
                .orElseGet(() -> ImmutableMap.of(LockState.NOT_PRESENT_ON_TIMELOCK, Instant.EPOCH));
        return ImmutableLockDigest.builder()
                .addAllLockDescriptors(lockDescriptors)
                .putAllLockStates(lockStates)
                .build();
    }

    private static LockDiagnosticInfoService createRpcClient(
            AtlasDbConfig config,
            Supplier<AtlasDbRuntimeConfig> runtimeConfigSupplier) {
        Supplier<ServerListConfig> serverListConfigSupplier =
                getServerListConfigSupplierForTimeLock(config, runtimeConfigSupplier);

        timelockNamespace(config);

        ServiceCreator serviceCreator = ServiceCreator.withPayloadLimiter(
                new MetricsManager(new MetricRegistry(), new DefaultTaggedMetricRegistry(), _unused -> true),
                serverListConfigSupplier,
                UserAgent.of(UserAgent.Agent.of("agent", "0.0.0")),
                () -> runtimeConfigSupplier.get().remotingClient());

        return serviceCreator.createService(LockDiagnosticInfoService.class);
    }

    private static String timelockNamespace(AtlasDbConfig config) {
        return OptionalResolver.resolve(
                config.timelock().flatMap(TimeLockClientConfig::client), config.namespace());
    }

    private static Supplier<ServerListConfig> getServerListConfigSupplierForTimeLock(
            AtlasDbConfig config,
            Supplier<AtlasDbRuntimeConfig> runtimeConfigSupplier) {
        TimeLockClientConfig clientConfig = config.timelock()
                .orElseGet(() -> ImmutableTimeLockClientConfig.builder().build());
        return () -> ServerListConfigs.parseInstallAndRuntimeConfigs(
                clientConfig,
                () -> runtimeConfigSupplier.get().timelockRuntime());
    }


}

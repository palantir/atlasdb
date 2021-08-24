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
import com.palantir.atlasdb.factory.ServiceCreator;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.timelock.api.ConjureLockDescriptor;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.persist.Persistable;
import com.palantir.common.streams.KeyedStream;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.refreshable.Refreshable;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import com.palantir.util.OptionalResolver;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * TODO(fdesouza): Remove this once PDS-95791 is resolved.
 * @deprecated Remove this once PDS-95791 is resolved.
 */
@Deprecated
public class TransactionPostMortemRunner {

    private static final SafeLogger log = SafeLoggerFactory.get(TransactionPostMortemRunner.class);

    private final String timelockNamespace;
    private final ClientLockDiagnosticCollector clientLockDiagnosticCollector;
    private final WritesDigestEmitter writesDigestEmitter;
    private final LockDiagnosticInfoService timelockDiagnosticService;
    private final LocalLockTracker localLockTracker;

    public TransactionPostMortemRunner(
            TransactionManager transactionManager,
            TableReference tableReference,
            AtlasDbConfig install,
            Refreshable<AtlasDbRuntimeConfig> runtime,
            ClientLockDiagnosticCollector clientLockDiagnosticCollector,
            LocalLockTracker localLockTracker) {
        this.timelockNamespace = timelockNamespace(install);
        this.clientLockDiagnosticCollector = clientLockDiagnosticCollector;
        this.timelockDiagnosticService = createRpcClient(install, runtime);
        this.writesDigestEmitter = new WritesDigestEmitter(transactionManager, tableReference);
        this.localLockTracker = localLockTracker;
    }

    public FullDiagnosticDigest<String> conductPostMortem(Persistable row, byte[] columnName) {
        WritesDigest<String> digest = writesDigestEmitter.getDigest(row, columnName);
        log.info("raw digest", SafeArg.of("rawDigest", digest));
        Map<Long, ClientLockDiagnosticDigest> snapshot = clientLockDiagnosticCollector.getSnapshot();
        log.info("client lock diagnostic digest", SafeArg.of("clientLockDiagnosticDigest", snapshot));
        Optional<LockDiagnosticInfo> lockDiagnosticInfo = getTimelockDiagnostics(snapshot);
        log.info("lock diagnostic info", SafeArg.of("timelockLockDiagnosticInfo", lockDiagnosticInfo));
        Set<UUID> lockRequestIdsEvictedMidLockRequest = lockDiagnosticInfo
                .map(LockDiagnosticInfo::requestIdsEvictedMidLockRequest)
                .orElseGet(ImmutableSet::of);

        log.info(
                "lock Request Ids Evicted Mid Lock Request",
                SafeArg.of("lockRequestIdsEvictedMidLockRequest", lockRequestIdsEvictedMidLockRequest));

        Set<FullDiagnosticDigest.CompletedTransactionDigest<String>> transactionDigests =
                digest.completedOrAbortedTransactions().keySet().stream()
                        .map(startTimestamp -> transactionDigest(
                                startTimestamp,
                                digest,
                                lockDiagnosticInfo,
                                snapshot.getOrDefault(startTimestamp, ClientLockDiagnosticDigest.missingEntry())))
                        .collect(Collectors.toSet());

        log.info("transaction digests", SafeArg.of("transactionDigests", transactionDigests));

        List<LocalLockTracker.TrackedLockEvent> locallyTrackedLockEvents = localLockTracker.getLocalLockHistory();

        return ImmutableFullDiagnosticDigest.<String>builder()
                .rawData(ImmutableRawData.of(digest, lockDiagnosticInfo, snapshot))
                .addAllInProgressTransactions(digest.inProgressTransactions())
                .lockRequestIdsEvictedMidLockRequest(lockRequestIdsEvictedMidLockRequest)
                .completedTransactionDigests(transactionDigests)
                .trackedLockEvents(locallyTrackedLockEvents)
                .build();
    }

    private Optional<LockDiagnosticInfo> getTimelockDiagnostics(Map<Long, ClientLockDiagnosticDigest> snapshot) {
        try {
            return timelockDiagnosticService.getEnhancedLockDiagnosticInfo(timelockNamespace, requestIds(snapshot));
        } catch (Exception e) {
            log.warn("recieved exception whilst trying to fetch timelock diagnostics", e);
            return Optional.empty();
        }
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

    private static <T> FullDiagnosticDigest.CompletedTransactionDigest<T> transactionDigest(
            long startTimestamp,
            WritesDigest<T> writesDigest,
            Optional<LockDiagnosticInfo> timelockLockInfo,
            ClientLockDiagnosticDigest clientLockDigest) {
        Map<UUID, Set<ConjureLockDescriptor>> lockRequests = ImmutableMap.<UUID, Set<ConjureLockDescriptor>>builder()
                .putAll(clientLockDigest.lockRequests())
                .put(clientLockDigest.immutableTimestampRequestId(), ImmutableSet.of())
                .build();
        Map<UUID, LockDigest> lockDigests = KeyedStream.stream(lockRequests)
                .map((requestId, descriptors) ->
                        lockDigest(descriptors, timelockLockInfo.map(info -> lockState(requestId, info))))
                .collectToMap();

        return ImmutableCompletedTransactionDigest.<T>builder()
                .startTimestamp(startTimestamp)
                .commitTimestamp(writesDigest.completedOrAbortedTransactions().get(startTimestamp))
                .value(writesDigest.allWrittenValuesDeserialized().get(startTimestamp))
                .immutableTimestamp(clientLockDigest.immutableTimestamp())
                .immutableTimestampLockRequestId(clientLockDigest.immutableTimestampRequestId())
                .locks(lockDigests)
                .addAllConflictTrace(clientLockDigest.writeWriteConflictTrace())
                .build();
    }

    private static Map<LockState, Instant> lockState(UUID requestId, LockDiagnosticInfo diagnosticInfo) {
        return diagnosticInfo.lockInfos().get(requestId).lockStates();
    }

    private static LockDigest lockDigest(
            Set<ConjureLockDescriptor> lockDescriptors, Optional<Map<LockState, Instant>> maybeLockStates) {
        Map<LockState, Instant> lockStates =
                maybeLockStates.orElseGet(() -> ImmutableMap.of(LockState.NOT_PRESENT_ON_TIMELOCK, Instant.EPOCH));
        return ImmutableLockDigest.builder()
                .addAllLockDescriptors(lockDescriptors)
                .putAllLockStates(lockStates)
                .build();
    }

    private static LockDiagnosticInfoService createRpcClient(
            AtlasDbConfig config, Refreshable<AtlasDbRuntimeConfig> runtimeConfigSupplier) {
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
        return OptionalResolver.resolve(config.timelock().flatMap(TimeLockClientConfig::client), config.namespace());
    }

    private static Supplier<ServerListConfig> getServerListConfigSupplierForTimeLock(
            AtlasDbConfig config, Refreshable<AtlasDbRuntimeConfig> runtimeConfigSupplier) {
        TimeLockClientConfig clientConfig = config.timelock()
                .orElseGet(() -> ImmutableTimeLockClientConfig.builder().build());
        return ServerListConfigs.parseInstallAndRuntimeConfigs(
                clientConfig, runtimeConfigSupplier.map(AtlasDbRuntimeConfig::timelockRuntime));
    }
}

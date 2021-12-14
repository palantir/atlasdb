/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.backup;

import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Token;
import com.datastax.driver.core.TokenRange;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Range;
import com.palantir.atlasdb.backup.api.CompletedBackup;
import com.palantir.atlasdb.backup.transaction.FullyBoundedTimestampRange;
import com.palantir.atlasdb.backup.transaction.TransactionsTableInteraction;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.backup.CassandraRepairHelper;
import com.palantir.atlasdb.cassandra.backup.ClusterMetadataUtils;
import com.palantir.atlasdb.internalschema.InternalSchemaMetadataState;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.cassandra.LightweightOppToken;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class AtlasRestoreService {
    private static final SafeLogger log = SafeLoggerFactory.get(AtlasRestoreService.class);

    private final BackupPersister backupPersister;
    private final CassandraRepairHelper cassandraRepairHelper;

    @VisibleForTesting
    AtlasRestoreService(BackupPersister backupPersister, CassandraRepairHelper cassandraRepairHelper) {
        this.backupPersister = backupPersister;
        this.cassandraRepairHelper = cassandraRepairHelper;
    }

    public static AtlasRestoreService create(
            BackupPersister backupPersister,
            Function<Namespace, CassandraKeyValueServiceConfig> keyValueServiceConfigFactory,
            Function<Namespace, KeyValueService> keyValueServiceFactory) {
        CassandraRepairHelper cassandraRepairHelper =
                new CassandraRepairHelper(keyValueServiceConfigFactory, keyValueServiceFactory);
        return new AtlasRestoreService(backupPersister, cassandraRepairHelper);
    }

    /**
     *  Returns the set of namespaces for which we successfully repaired internal tables.
     *  Only namespaces for which a known backup exists will be repaired.
     *  Namespaces are repaired serially. If repairTable throws an exception, then this will propagate back to the
     *  caller. In such cases, some namespaces may not have been repaired.
     *
     * @param namespaces the namespaces to repair.
     * @param repairTable supplied function which is expected to repair the given ranges.
     *
     * @return the set of namespaces for which we issued a repair command via the provided Consumer.
     */
    public Set<Namespace> repairInternalTables(
            Set<Namespace> namespaces, Consumer<Map<InetSocketAddress, Set<Range<LightweightOppToken>>>> repairTable) {
        Set<Namespace> namespacesToRepair =
                namespaces.stream().filter(this::backupExists).collect(Collectors.toSet());

        // ConsistentCasTablesTask
        namespacesToRepair.forEach(namespace -> cassandraRepairHelper.repairInternalTables(namespace, repairTable));

        // RepairTransactionsTablesTask
        namespacesToRepair.forEach(namespace -> repairTransactionsTables(namespace, repairTable));

        return namespacesToRepair;
    }

    // TODO(gs): move this into CassandraRepairHelper
    private void repairTransactionsTables(
            Namespace namespace, Consumer<Map<InetSocketAddress, Set<Range<LightweightOppToken>>>> repairTable) {
        // 1. get schema metadata
        Optional<InternalSchemaMetadataState> schemaMetadataState = backupPersister.getSchemaMetadata(namespace);
        CompletedBackup completedBackup =
                backupPersister.getCompletedBackup(namespace).orElseThrow();

        // 2. get txn tables interactions
        long fastForwardTs = completedBackup.getBackupEndTimestamp();

        // Do we need the immutable timestamp here?
        long immutableTs = completedBackup.getBackupStartTimestamp();

        Map<FullyBoundedTimestampRange, Integer> coordinationMap =
                CoordinationServiceUtilities.getCoordinationMapOnRestore(
                        schemaMetadataState, fastForwardTs, immutableTs);
        List<TransactionsTableInteraction> transactionsTableInteractions =
                TransactionsTableInteraction.getTransactionTableInteractions(
                        coordinationMap, DefaultRetryPolicy.INSTANCE);

        // 3. get partition tokens
        Map<String, Set<Token>> partitionKeysByTable =
                getPartitionKeysByTable(namespace, transactionsTableInteractions);

        if (log.isDebugEnabled()) {
            Map<String, FullyBoundedTimestampRange> loggableTableRanges = KeyedStream.of(transactionsTableInteractions)
                    .mapKeys(TransactionsTableInteraction::getTransactionsTableName)
                    .map(TransactionsTableInteraction::getTimestampRange)
                    .collectToMap();
            Map<String, Integer> numPartitionKeysByTable =
                    KeyedStream.stream(partitionKeysByTable).map(Set::size).collectToMap();
            log.debug(
                    "Identified token ranges requiring repair in the following transactions tables",
                    SafeArg.of("transactionsTablesWithRanges", loggableTableRanges),
                    SafeArg.of("numPartitionKeysByTable", numPartitionKeysByTable));
        }

        // 4. get repair ranges
        // TODO(gs): don't recreate a session here, we already got this information before
        Map<String, Map<InetSocketAddress, Set<Range<LightweightOppToken>>>> tokenRangesForRepair;
        try (Session session = cassandraRepairHelper.newSession(namespace)) {
            String keyspaceName = session.getLoggedKeyspace();
            Metadata metadata = session.getCluster().getMetadata();

            Set<InetSocketAddress> hosts = new HashSet<>(); // TODO(gs): move to somewhere where we have the hosts!!
            tokenRangesForRepair = KeyedStream.stream(partitionKeysByTable)
                    .map(ranges -> ClusterMetadataUtils.getTokenMapping(hosts, metadata, keyspaceName, ranges))
                    .map(this::makeLightweight)
                    .collectToMap();
        }

        // 5. repair ranges
        KeyedStream.stream(tokenRangesForRepair).forEach((table, ranges) -> {
            log.info("Repairing ranges for table", SafeArg.of("table", table));
            repairTable.accept(ranges);
        });
    }

    // TODO(gs): ugh. this gives us a stream in a stream in a stream
    private Map<InetSocketAddress, Set<Range<LightweightOppToken>>> makeLightweight(
            Map<InetSocketAddress, Set<TokenRange>> ranges) {
        return KeyedStream.stream(ranges)
                .map(cassandraRepairHelper::makeLightweight)
                .collectToMap();
    }

    private Map<String, Set<Token>> getPartitionKeysByTable(
            Namespace namespace, List<TransactionsTableInteraction> transactionsTableInteractions) {
        try (Session session = cassandraRepairHelper.newSession(namespace)) {
            String keyspaceName = session.getLoggedKeyspace();
            Metadata metadata = session.getCluster().getMetadata();
            return KeyedStream.of(transactionsTableInteractions.stream())
                    .mapKeys(TransactionsTableInteraction::getTransactionsTableName)
                    .map(interaction -> interaction.getPartitionTokens(
                            ClusterMetadataUtils.getTableMetadata(
                                    metadata, keyspaceName, interaction.getTransactionsTableName()),
                            session))
                    .collectToMap();
        }
    }

    private boolean backupExists(Namespace namespace) {
        Optional<CompletedBackup> maybeCompletedBackup = backupPersister.getCompletedBackup(namespace);

        if (maybeCompletedBackup.isEmpty()) {
            log.error("Could not restore namespace, as no backup is stored", SafeArg.of("namespace", namespace));
            return false;
        }

        return true;
    }
}

/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.base.Function;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.visitor.Visitor;
import com.palantir.remoting3.tracing.Tracers;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public final class CqlKeyValueServices {
    private static final Logger log = LoggerFactory.getLogger(CqlKeyValueService.class); // not a typo

    // this is used as a fallback when the user is using small server-side limiting of batches
    static final int UNCONFIGURED_DEFAULT_BATCH_SIZE_BYTES = 50 * 1024;
    static final Function<Entry<Cell, Value>, Long> PUT_ENTRY_SIZING_FUNCTION = input ->
            input.getValue().getContents().length
                    + CassandraConstants.TS_SIZE
                    + Cells.getApproxSizeOfCell(input.getKey());

    static final Function<Entry<Cell, byte[]>, Long> MULTIPUT_ENTRY_SIZING_FUNCTION = entry ->
            entry.getValue().length + Cells.getApproxSizeOfCell(entry.getKey());

    public void shutdown() {
        traceRetrievalExec.shutdown();
    }

    enum TransactionType {
        NONE,
        LIGHTWEIGHT_TRANSACTION_REQUIRED
    }

    private final ExecutorService traceRetrievalExec = Tracers.wrap(PTExecutors.newFixedThreadPool(8));

    private static final int MAX_TRIES = 20;
    private static final long TRACE_RETRIEVAL_MS_BETWEEN_TRIES = 500;

    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    public void logTracedQuery(
            String tracedQuery,
            ResultSet resultSet,
            Session session,
            LoadingCache<String, PreparedStatement> statementCache) {
        if (log.isInfoEnabled()) {

            List<ExecutionInfo> allExecutionInfo = Lists.newArrayList(resultSet.getAllExecutionInfo());
            for (final ExecutionInfo info : allExecutionInfo) {
                if (info.getQueryTrace() == null) {
                    continue;
                }
                final UUID traceId = info.getQueryTrace().getTraceId();
                log.info("Traced query {} with trace uuid {}", tracedQuery, traceId);
                traceRetrievalExec.submit((Callable<Void>) () -> {
                    StringBuilder sb = new StringBuilder();
                    sb.append("Retrieving traced query " + tracedQuery + " trace uuid: "
                            + traceId);
                    int tries = 0;
                    boolean success = false;

                    while (tries < MAX_TRIES) {
                        ResultSetFuture sessionFuture = session.executeAsync(
                                statementCache.getUnchecked(
                                        "SELECT * FROM system_traces.sessions WHERE session_id = ?").bind(traceId));

                        Row sessionRow = sessionFuture.getUninterruptibly().one();

                        if (sessionRow != null && !sessionRow.isNull("duration")) {
                            ResultSetFuture eventFuture = session.executeAsync(statementCache.getUnchecked(
                                    "SELECT * FROM system_traces.events WHERE session_id = ?")
                                    .bind(traceId));
                            List<Row> eventRows = eventFuture.getUninterruptibly().all();

                            sb.append(" requestType: ").append(sessionRow.getString("request"));
                            sb.append(" coordinator: ").append(sessionRow.getInet("coordinator"));
                            sb.append(" started_at: ").append(sessionRow.getTime("started_at"));
                            sb.append(" duration: ").append(sessionRow.getInt("duration"));
                            if (!sessionRow.isNull("parameters")) {
                                sb.append("\nparameters: "
                                        + Collections.unmodifiableMap(sessionRow.getMap(
                                        "parameters",
                                        String.class,
                                        String.class)));
                            }

                            for (Row eventRow : eventRows) {
                                sb.append(eventRow.getString("activity"))
                                        .append(" on ")
                                        .append(eventRow.getInet("source")).append("[")
                                        .append(eventRow.getString("thread")).append("] at ")
                                        .append(eventRow.getUUID("event_id").timestamp()).append(" (")
                                        .append(eventRow.getInt("source_elapsed")).append(" elapsed)\n");
                            }
                            success = true;
                            break;
                        }
                        tries++;
                        Thread.sleep(TRACE_RETRIEVAL_MS_BETWEEN_TRIES);
                    }
                    if (!success) {
                        sb.append(" (retrieval timed out)");
                    }
                    log.info("Query trace: {}", sb);
                    return null;
                });
            }
        }
    }

    @SuppressWarnings("VisibilityModifier")
    static class Peer {
        InetAddress peer;
        String dataCenter;
        String rack;
        String releaseVersion;
        InetAddress rpcAddress;
        UUID schemaVersion;
        Set<String> tokens;
    }

    public static Set<Peer> getPeers(Session session) {
        PreparedStatement selectPeerInfo = session.prepare("select peer, data_center, rack,"
                + " release_version, rpc_address, schema_version, tokens from system.peers;");

        Set<Peer> peers = Sets.newHashSet();

        for (Row row : session.execute(selectPeerInfo.bind()).all()) {
            Peer peer = new Peer();
            peer.peer = row.getInet("peer");
            peer.dataCenter = row.getString("data_center");
            peer.rack = row.getString("rack");
            peer.releaseVersion = row.getString("release_version");
            peer.rpcAddress = row.getInet("rpc_address");
            peer.schemaVersion = row.getUUID("schema_version");
            peer.tokens = row.getSet("tokens", String.class);
            peers.add(peer);
        }

        return peers;
    }

    @SuppressWarnings("VisibilityModifier")
    static class Local {
        String dataCenter;
        String rack;
    }

    public static Local getLocal(Session session) {
        PreparedStatement selectLocalInfo = session.prepare(
                "select data_center, rack from system.local;");
        Row localRow = session.execute(selectLocalInfo.bind()).one();
        Local local = new Local();
        local.dataCenter = localRow.getString("data_center");
        local.rack = localRow.getString("rack");
        return local;
    }

    public static void waitForSchemaVersionsToCoalesce(
            String encapsulatingOperationDescription,
            CqlKeyValueService kvs) {
        PreparedStatement peerInfoQuery = kvs.getPreparedStatement(
                CassandraConstants.NO_TABLE,
                "select peer, schema_version from system.peers;",
                kvs.session);
        peerInfoQuery.setConsistencyLevel(ConsistencyLevel.ALL);

        Multimap<UUID, InetAddress> peerInfo = ArrayListMultimap.create();
        long start = System.currentTimeMillis();
        long sleepTime = 100;
        do {
            peerInfo.clear();
            for (Row row : kvs.session.execute(peerInfoQuery.bind()).all()) {
                peerInfo.put(row.getUUID("schema_version"), row.getInet("peer"));
            }

            if (peerInfo.keySet().size() <= 1) { // full schema agreement
                return;
            }
            sleepTime = Math.min(sleepTime * 2, 5000);
        } while (System.currentTimeMillis() < start + CassandraConstants.SECONDS_WAIT_FOR_VERSIONS * 1000);

        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Cassandra cluster cannot come to agreement on schema versions,"
                + " during operation: %s.", encapsulatingOperationDescription));

        for (Entry<UUID, Collection<InetAddress>> versionToPeer : peerInfo.asMap().entrySet()) {
            sb.append(String.format("%nAt schema version %s:", versionToPeer.getKey()));
            for (InetAddress peer : versionToPeer.getValue()) {
                sb.append(String.format("%n\tNode: %s", peer));
            }
        }
        sb.append("\nFind the nodes above that diverge from the majority schema"
                + " (or have schema 'UNKNOWN', which likely means they are down/unresponsive)"
                + " and examine their logs to determine the issue. Fixing the underlying issue and restarting Cassandra"
                + " should resolve the problem. You can quick-check this with 'nodetool describecluster'.");
        throw new IllegalStateException(sb.toString());
    }

    void createTableWithSettings(TableReference tableRef, byte[] rawMetadata, CqlKeyValueService kvs) {
        StringBuilder queryBuilder = new StringBuilder();

        int explicitCompressionBlockSizeKb = 0;
        boolean negativeLookups = false;
        double falsePositiveChance = CassandraConstants.DEFAULT_LEVELED_COMPACTION_BLOOM_FILTER_FP_CHANCE;
        boolean appendHeavyAndReadLight = false;


        if (rawMetadata != null && rawMetadata.length != 0) {
            TableMetadata tableMetadata = TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(rawMetadata);
            explicitCompressionBlockSizeKb = tableMetadata.getExplicitCompressionBlockSizeKB();
            negativeLookups = tableMetadata.hasNegativeLookups();
            appendHeavyAndReadLight = tableMetadata.isAppendHeavyAndReadLight();
        }

        if (negativeLookups) {
            falsePositiveChance = CassandraConstants.NEGATIVE_LOOKUPS_BLOOM_FILTER_FP_CHANCE;
        } else if (appendHeavyAndReadLight) {
            falsePositiveChance = CassandraConstants.DEFAULT_SIZE_TIERED_COMPACTION_BLOOM_FILTER_FP_CHANCE;
        }

        int chunkLength = AtlasDbConstants.MINIMUM_COMPRESSION_BLOCK_SIZE_KB;
        if (explicitCompressionBlockSizeKb != 0) {
            chunkLength = explicitCompressionBlockSizeKb;
        }

        queryBuilder.append("CREATE TABLE IF NOT EXISTS " + kvs.getFullTableName(tableRef) + " ( "
                + kvs.fieldNameProvider.row() + " blob, "
                + kvs.fieldNameProvider.column() + " blob, "
                + kvs.fieldNameProvider.timestamp() + " bigint, "
                + kvs.fieldNameProvider.value() + " blob, "
                + "PRIMARY KEY ("
                + kvs.fieldNameProvider.row() + ", "
                + kvs.fieldNameProvider.column() + ", "
                + kvs.fieldNameProvider.timestamp() + ")) "
                + "WITH COMPACT STORAGE ");
        queryBuilder.append("AND " + "bloom_filter_fp_chance = " + falsePositiveChance + " ");
        queryBuilder.append("AND caching = '{\"keys\":\"ALL\", \"rows_per_partition\":\"ALL\"}' ");
        if (appendHeavyAndReadLight) {
            queryBuilder.append("AND compaction = { 'class': '"
                    + CassandraConstants.SIZE_TIERED_COMPACTION_STRATEGY + "'} ");
        } else {
            queryBuilder.append("AND compaction = {'sstable_size_in_mb': '80', 'class': '"
                    + CassandraConstants.LEVELED_COMPACTION_STRATEGY + "'} ");
        }
        queryBuilder.append("AND compression = {'chunk_length_kb': '" + chunkLength + "', "
                + "'sstable_compression': '" + CassandraConstants.DEFAULT_COMPRESSION_TYPE + "'}");
        queryBuilder.append("AND CLUSTERING ORDER BY ("
                + kvs.fieldNameProvider.column() + " ASC, "
                + kvs.fieldNameProvider.timestamp() + " ASC) ");

        BoundStatement createTableStatement =
                kvs.getPreparedStatement(tableRef, queryBuilder.toString(), kvs.longRunningQuerySession)
                        .setConsistencyLevel(ConsistencyLevel.ALL)
                        .bind();
        try {
            ResultSet resultSet = kvs.longRunningQuerySession.execute(createTableStatement);
            logTracedQuery(queryBuilder.toString(), resultSet, kvs.session, kvs.cqlStatementCache.normalQuery);
        } catch (Throwable t) {
            throw Throwables.throwUncheckedException(t);
        }
    }

    static void setSettingsForTable(TableReference tableRef, byte[] rawMetadata, CqlKeyValueService kvs) {
        int explicitCompressionBlockSizeKb = 0;
        boolean negativeLookups = false;
        double falsePositiveChance = CassandraConstants.DEFAULT_LEVELED_COMPACTION_BLOOM_FILTER_FP_CHANCE;
        boolean appendHeavyAndReadLight = false;

        if (rawMetadata != null && rawMetadata.length != 0) {
            TableMetadata tableMetadata = TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(rawMetadata);
            explicitCompressionBlockSizeKb = tableMetadata.getExplicitCompressionBlockSizeKB();
            negativeLookups = tableMetadata.hasNegativeLookups();
            appendHeavyAndReadLight = tableMetadata.isAppendHeavyAndReadLight();
        }

        if (negativeLookups) {
            falsePositiveChance = CassandraConstants.NEGATIVE_LOOKUPS_BLOOM_FILTER_FP_CHANCE;
        } else if (appendHeavyAndReadLight) {
            falsePositiveChance = CassandraConstants.DEFAULT_SIZE_TIERED_COMPACTION_BLOOM_FILTER_FP_CHANCE;
        }

        int chunkLength = AtlasDbConstants.MINIMUM_COMPRESSION_BLOCK_SIZE_KB;
        if (explicitCompressionBlockSizeKb != 0) {
            chunkLength = explicitCompressionBlockSizeKb;
        }

        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE " + kvs.getFullTableName(tableRef) + " WITH "
                + "bloom_filter_fp_chance = " + falsePositiveChance + " ");

        sb.append("AND caching = '{\"keys\":\"ALL\", \"rows_per_partition\":\"ALL\"}' ");

        if (appendHeavyAndReadLight) {
            sb.append("AND compaction = { 'class': '" + CassandraConstants.SIZE_TIERED_COMPACTION_STRATEGY + "'} ");
        } else {
            sb.append("AND compaction = {'sstable_size_in_mb': '80', 'class': '"
                    + CassandraConstants.LEVELED_COMPACTION_STRATEGY + "'} ");
        }
        sb.append("AND compression = {'chunk_length_kb': '" + chunkLength + "', "
                + "'sstable_compression': '" + CassandraConstants.DEFAULT_COMPRESSION_TYPE + "'}");

        BoundStatement alterTableStatement =
                kvs.getPreparedStatement(tableRef, sb.toString(), kvs.longRunningQuerySession)
                        .setConsistencyLevel(ConsistencyLevel.ALL)
                        .bind();
        try {
            kvs.longRunningQuerySession.execute(alterTableStatement);
        } catch (Throwable t) {
            throw Throwables.throwUncheckedException(t);
        }
    }


    interface ThreadSafeCqlResultVisitor extends Visitor<Multimap<Cell, Value>> {
        // marker
    }

    static class StartTsResultsCollector implements ThreadSafeCqlResultVisitor {
        private final Map<Cell, Value> collectedResults = Maps.newConcurrentMap();
        private final ValueExtractor extractor = new ValueExtractor(collectedResults);
        private final long startTs;

        StartTsResultsCollector(long startTs) {
            this.startTs = startTs;
        }

        public Map<Cell, Value> getCollectedResults() {
            return collectedResults;
        }

        @Override
        public void visit(Multimap<Cell, Value> results) {
            for (Entry<Cell, Value> e : results.entries()) {
                if (results.get(e.getKey()).size() > 1) {
                    throw new IllegalStateException("Too many results retrieved for cell " + e.getKey());
                }
                collectedResults.put(e.getKey(), e.getValue());
            }
        }
    }

    static class AllTimestampsCollector implements ThreadSafeCqlResultVisitor {
        private final Multimap<Cell, Long> collectedResults = HashMultimap.create();

        public Multimap<Cell, Long> getCollectedResults() {
            return collectedResults;
        }

        @Override
        public synchronized void visit(Multimap<Cell, Value> results) {
            for (Entry<Cell, Value> e : results.entries()) {
                collectedResults.put(e.getKey(), e.getValue().getTimestamp());
            }
        }
    }

    @SuppressWarnings("checkstyle:RegexpSinglelineJava")
    static Cell getMetadataCell(TableReference tableRef) {
        return Cell.create(
                tableRef.getQualifiedName().getBytes(Charset.defaultCharset()),
                "m".getBytes(StandardCharsets.UTF_8));
    }
}

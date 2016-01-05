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
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.util.Visitor;

public class CQLKeyValueServices {
    private static final Logger log = LoggerFactory.getLogger(CQLKeyValueService.class); // not a typo




    static final Function<Entry<Cell, Value>, Long> PUT_ENTRY_SIZING_FUNCTION = new Function<Entry<Cell, Value>, Long>() {
        @Override
        public Long apply(Entry<Cell, Value> input) {
            return input.getValue().getContents().length + CassandraConstants.TS_SIZE
                    + Cells.getApproxSizeOfCell(input.getKey());
        }
    };

    static final Function<Entry<Cell, byte[]>, Long> MULTIPUT_ENTRY_SIZING_FUNCTION = new Function<Entry<Cell, byte[]>, Long>() {
        @Override
        public Long apply(Entry<Cell, byte[]> entry) {
            long totalSize = 0;
            totalSize += entry.getValue().length;
            totalSize += Cells.getApproxSizeOfCell(entry.getKey());
            return totalSize;
        }
    };

    static enum TransactionType {
        NONE,
        LIGHTWEIGHT_TRANSACTION_REQUIRED
    }

    static final ExecutorService traceRetrievalExec = PTExecutors.newFixedThreadPool(8);
    private static final int MAX_TRIES = 20;
    private static final long TRACE_RETRIEVAL_MS_BETWEEN_TRIES = 500;


    public static void logTracedQuery(final String tracedQuery, ResultSet resultSet, final Session session,  final LoadingCache<String, PreparedStatement> statementCache) {
        if (log.isInfoEnabled()) {

            List<ExecutionInfo> allExecutionInfo = Lists.newArrayList(resultSet.getAllExecutionInfo());
            for (final ExecutionInfo info : allExecutionInfo) {
                if (info.getQueryTrace() == null) {
                    continue;
                }
                final UUID traceId = info.getQueryTrace().getTraceId();
                log.info("Traced query " + tracedQuery + " with trace uuid " + traceId);
                traceRetrievalExec.submit(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
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
                                ResultSetFuture eventFuture = session.executeAsync(
                                        statementCache.getUnchecked(
                                                "SELECT * FROM system_traces.events WHERE session_id = ?").bind(traceId));
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
                        log.info(sb.toString());
                        return null;
                    }
                });
            }
        }
    }

    static class Peer {
        InetAddress peer;
        String data_center;
        String rack;
        String release_version;
        InetAddress rpc_address;
        UUID schema_version;
        Set<String> tokens;
    }

    public static Set<Peer> getPeers(Session session) {
        PreparedStatement selectPeerInfo = session.prepare(
                "select peer, data_center, rack, release_version, rpc_address, schema_version, tokens from system.peers;");

        Set<Peer> peers = Sets.newHashSet();

        for (Row row: session.execute(selectPeerInfo.bind()).all()) {
            Peer peer = new Peer();
            peer.peer = row.getInet("peer");
            peer.data_center = row.getString("data_center");
            peer.rack = row.getString("rack");
            peer.release_version = row.getString("release_version");
            peer.rpc_address = row.getInet("rpc_address");
            peer.schema_version = row.getUUID("schema_version");
            peer.tokens = row.getSet("tokens", String.class);
            peers.add(peer);
        }

        return peers;
    }

    public static void waitForSchemaVersionsToCoalesce(String encapsulatingOperationDescription, CQLKeyValueService kvs) {
        PreparedStatement peerInfoQuery = kvs.getPreparedStatement(CassandraConstants.NO_TABLE, "select peer, schema_version from system.peers;", kvs.session);
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
        } while (System.currentTimeMillis() < start + CassandraConstants.SECONDS_WAIT_FOR_VERSIONS*1000);

        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Cassandra cluster cannot come to agreement on schema versions, during operation: %s.", encapsulatingOperationDescription));

        for ( Entry<UUID, Collection<InetAddress>> versionToPeer : peerInfo.asMap().entrySet()) {
            sb.append(String.format("\nAt schema version %s:", versionToPeer.getKey()));
            for (InetAddress peer: versionToPeer.getValue()) {
                sb.append(String.format("\n\tNode: %s", peer));
            }
        }
        sb.append("\nFind the nodes above that diverge from the majority schema " +
                "(or have schema 'UNKNOWN', which likely means they are down/unresponsive) " +
                "and examine their logs to determine the issue. Fixing the underlying issue and restarting Cassandra " +
                "should resolve the problem. You can quick-check this with 'nodetool describecluster'.");
        throw new IllegalStateException(sb.toString());
    }

    static byte[] getRowName(Row row) {
        return CassandraKeyValueServices.getBytesFromByteBuffer(row.getBytes(CassandraConstants.ROW_NAME));
    }

    static byte[] getColName(Row row) {
        return CassandraKeyValueServices.getBytesFromByteBuffer(row.getBytes(CassandraConstants.COL_NAME_COL));
    }

    static long getTs(Row row) {
        return ~row.getLong(CassandraConstants.TS_COL);
    }

    static byte[] getValue(Row row) {
        return CassandraKeyValueServices.getBytesFromByteBuffer(row.getBytes(CassandraConstants.VALUE_COL));
    }

    static void createTableWithSettings(String tableName, byte[] rawMetadata, CQLKeyValueService kvs) {
        StringBuilder queryBuilder = new StringBuilder();

        int explicitCompressionBlockSizeKB = 0;
        boolean negativeLookups = false;
        double falsePositiveChance = CassandraConstants.DEFAULT_LEVELED_COMPACTION_BLOOM_FILTER_FP_CHANCE;
        boolean appendHeavyAndReadLight = false;


        if (rawMetadata != null && rawMetadata.length != 0) {
            TableMetadata tableMetadata = TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(rawMetadata);
            explicitCompressionBlockSizeKB = tableMetadata.getExplicitCompressionBlockSizeKB();
            negativeLookups = tableMetadata.hasNegativeLookups();
            appendHeavyAndReadLight = tableMetadata.isAppendHeavyAndReadLight();
        }

        if (negativeLookups) {
            falsePositiveChance = CassandraConstants.NEGATIVE_LOOKUPS_BLOOM_FILTER_FP_CHANCE;
        } else if (appendHeavyAndReadLight) {
            falsePositiveChance = CassandraConstants.DEFAULT_SIZE_TIERED_COMPACTION_BLOOM_FILTER_FP_CHANCE;
        }

        int chunkLength = AtlasDbConstants.MINIMUM_COMPRESSION_BLOCK_SIZE_KB;
        if (explicitCompressionBlockSizeKB != 0) {
            chunkLength = explicitCompressionBlockSizeKB;
        }

        queryBuilder.append("CREATE TABLE " + kvs.getFullTableName(tableName) + " ( " // full table name (ks.cf)
                + CassandraConstants.ROW_NAME + " blob, "
                + CassandraConstants.COL_NAME_COL + " blob, "
                + CassandraConstants.TS_COL + " bigint, "
                + CassandraConstants.VALUE_COL + " blob, "
                + "PRIMARY KEY ("
                + CassandraConstants.ROW_NAME + ", "
                + CassandraConstants.COL_NAME_COL + ", "
                + CassandraConstants.TS_COL + ")) "
                + "WITH COMPACT STORAGE ");
        queryBuilder.append("AND " + "bloom_filter_fp_chance = " + falsePositiveChance + " ");
        queryBuilder.append("AND caching = '{\"keys\":\"ALL\", \"rows_per_partition\":\"ALL\"}' ");
        if (appendHeavyAndReadLight) {
            queryBuilder.append("AND compaction = { 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'} ");
        } else {
            queryBuilder.append("AND compaction = {'sstable_size_in_mb': '80', 'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'} ");
        }
        queryBuilder.append("AND compression = {'chunk_length_kb': '" + chunkLength + "', "
                + "'sstable_compression': '" + CassandraConstants.DEFAULT_COMPRESSION_TYPE + "'}");
        queryBuilder.append("AND CLUSTERING ORDER BY ("
                + CassandraConstants.COL_NAME_COL + " ASC, "
                + CassandraConstants.TS_COL + " ASC) ");

        BoundStatement createTableStatement =
                kvs.getPreparedStatement(tableName, queryBuilder.toString(), kvs.longRunningQuerySession)
                        .setConsistencyLevel(ConsistencyLevel.ALL)
                        .bind();
        try {
            ResultSet resultSet = kvs.longRunningQuerySession.execute(createTableStatement);
            CQLKeyValueServices.logTracedQuery(queryBuilder.toString(), resultSet, kvs.session, kvs.cqlStatementCache.NORMAL_QUERY);
        } catch (Throwable t) {
            throw Throwables.throwUncheckedException(t);
        }
    }

    static void setSettingsForTable(String tableName, byte[] rawMetadata, CQLKeyValueService kvs) {
        int explicitCompressionBlockSizeKB = 0;
        boolean negativeLookups = false;
        double falsePositiveChance = CassandraConstants.DEFAULT_LEVELED_COMPACTION_BLOOM_FILTER_FP_CHANCE;
        boolean appendHeavyAndReadLight = false;

        if (rawMetadata != null && rawMetadata.length != 0) {
            TableMetadata tableMetadata = TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(rawMetadata);
            explicitCompressionBlockSizeKB = tableMetadata.getExplicitCompressionBlockSizeKB();
            negativeLookups = tableMetadata.hasNegativeLookups();
            appendHeavyAndReadLight = tableMetadata.isAppendHeavyAndReadLight();
        }

        if (negativeLookups) {
            falsePositiveChance = CassandraConstants.NEGATIVE_LOOKUPS_BLOOM_FILTER_FP_CHANCE;
        } else if (appendHeavyAndReadLight) {
            falsePositiveChance = CassandraConstants.DEFAULT_SIZE_TIERED_COMPACTION_BLOOM_FILTER_FP_CHANCE;
        }

        int chunkLength = AtlasDbConstants.MINIMUM_COMPRESSION_BLOCK_SIZE_KB;
        if (explicitCompressionBlockSizeKB != 0) {
            chunkLength = explicitCompressionBlockSizeKB;
        }

        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE " + kvs.getFullTableName(tableName) + " WITH "
                + "bloom_filter_fp_chance = " + falsePositiveChance + " ");

        sb.append("AND caching = '{\"keys\":\"ALL\", \"rows_per_partition\":\"ALL\"}' ");

        if (appendHeavyAndReadLight) {
            sb.append("AND compaction = { 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'} ");
        } else {
            sb.append("AND compaction = {'sstable_size_in_mb': '80', 'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'} ");
        }
        sb.append("AND compression = {'chunk_length_kb': '" + chunkLength + "', "
                + "'sstable_compression': '" + CassandraConstants.DEFAULT_COMPRESSION_TYPE + "'}");

        BoundStatement alterTableStatement =
                kvs.getPreparedStatement(tableName, sb.toString(), kvs.longRunningQuerySession)
                        .setConsistencyLevel(ConsistencyLevel.ALL)
                        .bind();
        try {
            kvs.longRunningQuerySession.execute(alterTableStatement);
        } catch (Throwable t) {
            throw Throwables.throwUncheckedException(t);
        }
    }


    static interface ThreadSafeCQLResultVisitor extends Visitor<Map<Cell, Value>> {
        // marker
    }

    static class StartTsResultsCollector implements ThreadSafeCQLResultVisitor {
        final Map<Cell, Value> collectedResults = Maps.newConcurrentMap();
        final ValueExtractor extractor = new ValueExtractor(collectedResults);
        final long startTs;

        public StartTsResultsCollector(long startTs) {
            this.startTs = startTs;
        }

        @Override
        public void visit(Map<Cell, Value> results) {
            collectedResults.putAll(results);
        }
    }

    static class AllTimestampsCollector implements ThreadSafeCQLResultVisitor {
        final Multimap<Cell, Long> collectedResults = HashMultimap.create();

        @Override
        public synchronized void visit(Map<Cell, Value> results) {
            for (Entry<Cell, Value> e : results.entrySet()) {
                collectedResults.put(e.getKey(), e.getValue().getTimestamp());
            }
        }
    }

    static Cell getMetadataCell(String tableName) {
        return Cell.create(tableName.getBytes(), "m".getBytes());
    }
}

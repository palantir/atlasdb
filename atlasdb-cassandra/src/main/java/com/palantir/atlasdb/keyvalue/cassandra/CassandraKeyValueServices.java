/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.keyvalue.cassandra;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.annotation.Output;
import com.palantir.common.base.RunnableCheckedException;
import com.palantir.common.base.Throwables;
import com.palantir.common.visitor.Visitor;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.util.Pair;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.thrift.TException;

public final class CassandraKeyValueServices {
    private static final SafeLogger log = SafeLoggerFactory.get(CassandraKeyValueServices.class);

    private static final long INITIAL_SLEEP_TIME = 100;
    private static final long MAX_SLEEP_TIME = 5000;
    public static final String VERSION_UNREACHABLE = "UNREACHABLE";
    public static final byte[] METADATA_COL = "m".getBytes(StandardCharsets.UTF_8);

    private CassandraKeyValueServices() {
        // Utility class
    }

    /**
     * Attempt to wait until a quorum of nodes has reached consensus on a schema version, and it is known that no
     * other nodes currently disagree with this quorum.
     * <p>
     * The goals of this method include:
     * <ol>
     *     <li>
     *         Backoff during schema mutations if the cluster is known to currently be in disagreement, so that
     *         Cassandra can more efficiently come to agreement.
     *     </li>
     *     <li>
     *         Avoid performing schema mutations if they could potentially cause a split-brain in terms of how the
     *         schema evolves. This is achieved by ensuring a quorum of reachable nodes agree on the version. There
     *         is of course a check-then-act race condition, but Cassandra is able to eventually recover.
     *     </li>
     *     <li>
     *         Allowing schema mutations to take place in the presence of failures (the KVS needs to be able to
     *         tolerate a limited number of Cassandra node-level failures).
     *     </li>
     * </ol>
     *
     * @param schemaMutationTimeMillis      time to wait for nodes' schema versions to match.
     * @param client                        Cassandra client.
     * @param unsafeSchemaChangeDescription description of the schema change that was performed prior to this check.
     * @throws IllegalStateException if we wait for more than schemaMutationTimeoutMillis specified in config.
     */
    static void waitForSchemaVersions(
            int schemaMutationTimeMillis, CassandraClient client, String unsafeSchemaChangeDescription)
            throws TException {
        long start = System.currentTimeMillis();
        long sleepTime = INITIAL_SLEEP_TIME;
        Map<String, List<String>> versions;
        do {
            // This may only include some of the nodes if the coordinator hasn't shaken hands with someone; however,
            // this existed largely as a defense against performance issues with concurrent schema modifications.
            versions = client.describe_schema_versions();
            if (majorityOfClusterHasConsistentSchemaVersionAndNoDivergentNodes(versions)) {
                return;
            }
            sleepTime = sleepAndGetNextBackoffTime(sleepTime);
        } while (System.currentTimeMillis() < start + schemaMutationTimeMillis);

        StringBuilder schemaVersions = new StringBuilder();
        for (Map.Entry<String, List<String>> version : versions.entrySet()) {
            addNodeInformation(
                    schemaVersions, String.format("%nAt schema version %s:", version.getKey()), version.getValue());
        }

        String clusterNodes = addNodeInformation(
                        new StringBuilder(),
                        "Nodes believed to exist:",
                        versions.values().stream().flatMap(Collection::stream).collect(Collectors.toList()))
                .toString();

        String errorMessage = String.format(
                "Cassandra cluster cannot come to agreement on schema versions, %s. %s"
                        + " \nFind the nodes above that diverge from the majority schema and examine their logs to"
                        + " determine the issue. If nodes have schema 'UNKNOWN', they are likely down/unresponsive."
                        + " Fixing the underlying issue and restarting Cassandra should resolve the problem."
                        + " You can quick-check this with 'nodetool describecluster'."
                        + " \nIf nodes are specified in the config file, but do not have a schema version listed"
                        + " above, then they may have never joined the cluster. Verify your configuration is correct"
                        + " and that the nodes specified in the config are up and joined the cluster. %s",
                unsafeSchemaChangeDescription, schemaVersions.toString(), clusterNodes);
        throw new IllegalStateException(errorMessage);
    }

    static void runWithWaitingForSchemas(
            RunnableCheckedException<TException> task,
            CassandraKeyValueServiceConfig config,
            CassandraClient client,
            String unsafeSchemaChangeDescription)
            throws TException {
        waitForSchemaVersions(config.schemaMutationTimeoutMillis(), client, "before " + unsafeSchemaChangeDescription);
        task.run();
        waitForSchemaVersions(config.schemaMutationTimeoutMillis(), client, "after " + unsafeSchemaChangeDescription);
    }

    private static boolean majorityOfClusterHasConsistentSchemaVersionAndNoDivergentNodes(
            Map<String, List<String>> versions) {
        if (getDistinctReachableSchemas(versions).size() != 1) {
            return false;
        }

        int totalNodes = versions.values().stream().mapToInt(List::size).sum();
        int numUnreachableNodes = Optional.ofNullable(versions.get(VERSION_UNREACHABLE))
                .map(List::size)
                .orElse(0);
        return totalNodes - numUnreachableNodes >= (totalNodes / 2) + 1;
    }

    private static List<String> getDistinctReachableSchemas(Map<String, List<String>> versions) {
        return versions.keySet().stream()
                .filter(schema -> !schema.equals(VERSION_UNREACHABLE))
                .collect(Collectors.toList());
    }

    private static long sleepAndGetNextBackoffTime(long sleepTime) {
        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Throwables.throwUncheckedException(e);
        }
        return Math.min(sleepTime * 2, MAX_SLEEP_TIME);
    }

    private static StringBuilder addNodeInformation(StringBuilder builder, String message, List<String> nodes) {
        builder.append(message);
        for (String node : nodes) {
            builder.append(String.format("%n\tNode: %s", node));
        }
        return builder;
    }

    /**
     * This is a request from pbrown / FDEs; basically it's a pain to do DB surgery to get out
     * of failed patch upgrades, the majority of which requires schema mutations; they would find
     * it preferable to stop before starting the actual patch upgrade / setting APPLYING state.
     */
    static void warnUserInInitializationIfClusterAlreadyInInconsistentState(
            CassandraClientPool clientPool, CassandraKeyValueServiceConfig config) {
        try {
            clientPool.run(client -> {
                waitForSchemaVersions(config.schemaMutationTimeoutMillis(), client, " during an initialization check");
                return null;
            });
        } catch (Exception e) {
            log.warn("Failed to retrieve current Cassandra cluster schema status.", e);
        }
    }

    static String encodeAsHex(byte[] array) {
        return "0x" + PtBytes.encodeHexString(array);
    }

    @SuppressWarnings("BadAssert") // performance sensitive asserts
    public static ByteBuffer makeCompositeBuffer(byte[] colName, long positiveTimestamp) {
        assert colName.length <= 1 << 16 : "Cannot use column names larger than 64KiB, was " + colName.length;

        ByteBuffer buffer = ByteBuffer.allocate(6 /* misc */ + 8 /* timestamp */ + colName.length)
                .order(ByteOrder.BIG_ENDIAN);

        buffer.put((byte) ((colName.length >> 8) & 0xFF));
        buffer.put((byte) (colName.length & 0xFF));
        buffer.put(colName);
        buffer.put((byte) 0);

        buffer.put((byte) 0);
        buffer.put((byte) (8 & 0xFF));
        buffer.putLong(~positiveTimestamp);
        buffer.put((byte) 0);

        buffer.flip();

        return buffer;
    }

    static Pair<byte[], Long> decompose(ByteBuffer inputComposite) {
        ByteBuffer composite = inputComposite.slice().order(ByteOrder.BIG_ENDIAN);

        short len = composite.getShort();
        byte[] colName = new byte[len];
        composite.get(colName);

        short shouldBeZero = composite.getShort();
        com.palantir.logsafe.Preconditions.checkArgument(shouldBeZero == 0);

        byte shouldBe8 = composite.get();
        com.palantir.logsafe.Preconditions.checkArgument(shouldBe8 == 8);
        long ts = composite.getLong();

        return Pair.create(colName, ~ts);
    }

    /**
     * Convenience method to get the name buffer for the specified column and
     * decompose it into the name and timestamp.
     */
    public static Pair<byte[], Long> decomposeName(Column column) {
        ByteBuffer nameBuffer;
        if (column.isSetName()) {
            nameBuffer = column.bufferForName();
        } else {
            // the column buffer has not yet been set/cached
            // so we must fallback on the slowpath and force
            // the transform to bytes and wrap ourselves
            nameBuffer = ByteBuffer.wrap(column.getName());
        }
        return decompose(nameBuffer);
    }

    public static byte[] getBytesFromByteBuffer(ByteBuffer buffer) {
        // Be careful *NOT* to perform anything that will modify the buffer's position or limit
        byte[] bytes = new byte[buffer.limit() - buffer.position()];
        if (buffer.hasArray()) {
            System.arraycopy(buffer.array(), buffer.arrayOffset() + buffer.position(), bytes, 0, bytes.length);
        } else {
            buffer.duplicate().get(bytes, buffer.position(), bytes.length);
        }
        return bytes;
    }

    // /Obviously/ this is long (internal cassandra timestamp) + long (internal cassandra clock sequence and node id)
    static String convertCassandraByteBufferUuidToString(ByteBuffer uuid) {
        return new UUID(uuid.getLong(uuid.position()), uuid.getLong(uuid.position() + 8)).toString();
    }

    static String getFilteredStackTrace(String filter) {
        Exception ex = new Exception();
        StackTraceElement[] stackTrace = ex.getStackTrace();
        StringBuilder sb = new StringBuilder();
        for (StackTraceElement element : stackTrace) {
            if (element.getClassName().contains(filter)) {
                sb.append(element.toString()).append("\n");
            }
        }
        return sb.toString();
    }

    static Column createColumn(Cell cell, Value value) {
        return createColumnAtSpecificCassandraTimestamp(cell, value, value.getTimestamp());
    }

    /**
     * Creates a {@link Column} for an Atlas tombstone.
     * These columns have an Atlas timestamp of zero, but should not have a Cassandra timestamp of zero as that may
     * interfere with compactions. We want these to be at least reasonably consistent with Atlas's overall logical
     * time.
     * <p>
     * In practice, usage may involve obtaining a (reasonably) fresh timestamp and using that as the timestamp for the
     * deletion.
     */
    static Column createColumnForDelete(Cell cell, Value value, long cassandraTimestamp) {
        Preconditions.checkState(
                Arrays.equals(value.getContents(), PtBytes.EMPTY_BYTE_ARRAY),
                "Attempted to createColumnForDelete on a non-delete value, for cell %s and value %s",
                cell,
                value);
        return createColumnAtSpecificCassandraTimestamp(cell, value, cassandraTimestamp);
    }

    private static Column createColumnAtSpecificCassandraTimestamp(Cell cell, Value value, long cassandraTimestamp) {
        byte[] contents = value.getContents();
        long atlasTimestamp = value.getTimestamp();
        ByteBuffer colName = makeCompositeBuffer(cell.getColumnName(), atlasTimestamp);
        Column col = new Column();
        col.setName(colName);
        col.setValue(contents);
        col.setTimestamp(cassandraTimestamp);
        return col;
    }

    static Cell getMetadataCell(TableReference tableRef) {
        // would have preferred an explicit charset, but thrift uses default internally
        return Cell.create(lowerCaseTableReferenceToBytes(tableRef), METADATA_COL);
    }

    @SuppressWarnings("checkstyle:RegexpSinglelineJava")
    static Cell getOldMetadataCell(TableReference tableRef) {
        return Cell.create(tableRef.getQualifiedName().getBytes(Charset.defaultCharset()), METADATA_COL);
    }

    static RangeRequest metadataRangeRequest() {
        return RangeRequest.builder()
                .retainColumns(ImmutableSet.of(METADATA_COL))
                .build();
    }

    static RangeRequest metadataRangeRequestForTable(TableReference tableRef) {
        byte[] startRow = upperCaseTableReferenceToBytes(tableRef);
        byte[] endRow = lowerCaseTableReferenceToBytes(tableRef);
        return RangeRequest.builder()
                .startRowInclusive(startRow)
                .endRowExclusive(RangeRequests.nextLexicographicName(endRow))
                .retainColumns(ImmutableSet.of(METADATA_COL))
                .build();
    }

    @SuppressWarnings("checkstyle:RegexpSinglelineJava")
    static byte[] lowerCaseTableReferenceToBytes(TableReference tableRef) {
        return tableRef.getQualifiedName().toLowerCase().getBytes(Charset.defaultCharset());
    }

    @SuppressWarnings("checkstyle:RegexpSinglelineJava")
    static byte[] upperCaseTableReferenceToBytes(TableReference tableRef) {
        return tableRef.getQualifiedName().toUpperCase().getBytes(Charset.defaultCharset());
    }

    @SuppressWarnings("checkstyle:RegexpSinglelineJava")
    static TableReference lowerCaseTableReferenceFromBytes(byte[] name) {
        return TableReference.createUnsafe(new String(name, Charset.defaultCharset()).toLowerCase());
    }

    static TableReference tableReferenceFromCfDef(CfDef cf) {
        return TableReference.fromInternalTableName(cf.getName());
    }

    @SuppressWarnings("checkstyle:RegexpSinglelineJava")
    static TableReference tableReferenceFromBytes(byte[] name) {
        return TableReference.createUnsafe(new String(name, Charset.defaultCharset()));
    }

    interface ThreadSafeResultVisitor extends Visitor<Map<ByteBuffer, List<ColumnOrSuperColumn>>> {
        // marker
    }

    static class StartTsResultsCollector implements ThreadSafeResultVisitor {
        private final Map<Cell, Value> collectedResults = new ConcurrentHashMap<>();
        private final ValueExtractor extractor;
        private final long startTs;

        StartTsResultsCollector(MetricsManager metricsManager, long startTs) {
            this.extractor = new ValueExtractor(metricsManager, collectedResults);
            this.startTs = startTs;
        }

        public Map<Cell, Value> getCollectedResults() {
            return collectedResults;
        }

        @Override
        public void visit(Map<ByteBuffer, List<ColumnOrSuperColumn>> results) {
            extractor.extractResults(results, startTs, ColumnSelection.all());
        }
    }

    static class AllTimestampsCollector implements ThreadSafeResultVisitor {
        private final Multimap<Cell, Long> collectedResults = HashMultimap.create();

        public Multimap<Cell, Long> getCollectedResults() {
            return collectedResults;
        }

        @Override
        public synchronized void visit(Map<ByteBuffer, List<ColumnOrSuperColumn>> results) {
            extractTimestampResults(collectedResults, results);
        }
    }

    private static void extractTimestampResults(
            @Output Multimap<Cell, Long> ret, Map<ByteBuffer, List<ColumnOrSuperColumn>> results) {
        for (Map.Entry<ByteBuffer, List<ColumnOrSuperColumn>> result : results.entrySet()) {
            byte[] row = CassandraKeyValueServices.getBytesFromByteBuffer(result.getKey());
            for (ColumnOrSuperColumn col : result.getValue()) {
                Pair<byte[], Long> pair = CassandraKeyValueServices.decomposeName(col.column);
                ret.put(Cell.create(row, pair.lhSide), pair.rhSide);
            }
        }
    }

    static boolean isEmptyOrInvalidMetadata(byte[] metadata) {
        return metadata == null
                || Arrays.equals(metadata, AtlasDbConstants.EMPTY_TABLE_METADATA)
                || Arrays.equals(metadata, AtlasDbConstants.GENERIC_TABLE_METADATA);
    }

    static TableMetadata getMetadataOrDefaultToGeneric(byte[] metadata) {
        if (metadata == null || Arrays.equals(metadata, AtlasDbConstants.EMPTY_TABLE_METADATA)) {
            return TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(AtlasDbConstants.GENERIC_TABLE_METADATA);
        }
        return TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(metadata);
    }
}

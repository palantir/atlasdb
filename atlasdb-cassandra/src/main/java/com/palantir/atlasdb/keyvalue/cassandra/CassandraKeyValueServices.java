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

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.commons.lang3.Validate;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.annotation.Output;
import com.palantir.common.base.Throwables;
import com.palantir.common.visitor.Visitor;
import com.palantir.util.Pair;

public final class CassandraKeyValueServices {
    private static final Logger log = LoggerFactory.getLogger(CassandraKeyValueService.class); // did this on purpose

    private static final long INITIAL_SLEEP_TIME = 100;
    private static final long MAX_SLEEP_TIME = 5000;
    public static final String VERSION_UNREACHABLE = "UNREACHABLE";

    private CassandraKeyValueServices() {
        // Utility class
    }

    /**
     * Attempt to wait until nodes' schema versions match.
     *
     * @param config the KVS configuration.
     * @param client Cassandra client.
     * @param tableName table being modified.
     * @throws IllegalStateException if we wait for more than schemaMutationTimeoutMillis specified in config.
     */
    static String waitForSchemaVersions(
            CassandraKeyValueServiceConfig config,
            CassandraClient client,
            String tableName)
            throws TException {
        long start = System.currentTimeMillis();
        long sleepTime = INITIAL_SLEEP_TIME;
        Map<String, List<String>> versions;
        do {
            // This only returns the schema versions of nodes that the client knows exist. In particular, if a node we
            // shook hands with goes down, it will have schema version UNREACHABLE; however, if we never shook hands
            // with a node, there will simply be no entry for it in the map. Hence the check for the number of nodes.
            versions = client.describe_schema_versions();
            Optional<String> uniqueSchema = uniqueSchemaWithQuorumAgreementAndOtherNodesUnreachable(config, versions);
            if (uniqueSchema.isPresent()) {
                return uniqueSchema.get();
            }
            sleepTime = sleepWithExponentialBackoff(sleepTime);
        } while (System.currentTimeMillis() < start + config.schemaMutationTimeoutMillis());

        StringBuilder schemaVersions = new StringBuilder();
        for (Entry<String, List<String>> version : versions.entrySet()) {
            schemaVersions = addNodeInformation(schemaVersions,
                    String.format("%nAt schema version %s:", version.getKey()),
                    version.getValue());
        }

        String configNodes = addNodeInformation(new StringBuilder(),
                "Nodes specified in config file:",
                config.servers().stream().map(InetSocketAddress::getHostName).collect(Collectors.toList()))
                .toString();

        String errorMessage = String.format("Cassandra cluster cannot come to agreement on schema versions,"
                        + " after attempting to modify table %s. %s"
                        + " \nFind the nodes above that diverge from the majority schema and examine their logs to"
                        + " determine the issue. If nodes have schema 'UNKNOWN', they are likely down/unresponsive."
                        + " Fixing the underlying issue and restarting Cassandra should resolve the problem."
                        + " You can quick-check this with 'nodetool describecluster'."
                        + " \nIf nodes are specified in the config file, but do not have a schema version listed"
                        + " above, then they may have never joined the cluster. Verify your configuration is correct"
                        + " and that the nodes specified in the config are up and joined the cluster. %s",
                tableName,
                schemaVersions.toString(),
                configNodes);
        throw new IllegalStateException(errorMessage);
    }

    static Optional<String> getUniqueSchemaVersionIfQuorumAgreesAndOtherNodesUnreachable(
            CassandraKeyValueServiceConfig config,
            CassandraClient client) throws TException {
        return uniqueSchemaWithQuorumAgreementAndOtherNodesUnreachable(config, client.describe_schema_versions());
    }

    private static Optional<String> uniqueSchemaWithQuorumAgreementAndOtherNodesUnreachable(
            CassandraKeyValueServiceConfig config,
            Map<String, List<String>> versions) {
        List<String> reachableSchemas = getDistinctReachableSchemas(versions);
        if (reachableSchemas.size() > 1) {
            return Optional.empty();
        }

        int numberOfServers = config.servers().size();
        int numberOfVisibleNodes = getNumberOfReachableNodes(versions);

        if (numberOfVisibleNodes >= ((numberOfServers / 2) + 1)) {
            return Optional.of(Iterables.getOnlyElement(reachableSchemas));
        } else {
            return Optional.empty();
        }
    }

    private static List<String> getDistinctReachableSchemas(Map<String, List<String>> versions) {
        return versions.keySet().stream()
                .filter(schema -> !schema.equals(VERSION_UNREACHABLE))
                .collect(Collectors.toList());
    }

    private static int getNumberOfReachableNodes(Map<String, List<String>> versions) {
        return versions.entrySet().stream().filter(entry -> !entry.getKey().equals(VERSION_UNREACHABLE))
                .map(Entry::getValue)
                .mapToInt(List::size)
                .sum();
    }

    private static long sleepWithExponentialBackoff(long sleepTime) {
        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
            throw Throwables.throwUncheckedException(e);
        }
        return  Math.min(sleepTime * 2, MAX_SLEEP_TIME);
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
            CassandraClientPool clientPool,
            CassandraKeyValueServiceConfig config) {
        try {
            clientPool.run(client -> {
                waitForSchemaVersions(config, client, "(none, just an initialization check)");
                return null;
            });
        } catch (Exception e) {
            log.warn("Failed to retrieve current Cassandra cluster schema status.", e);
        }
    }

    static String encodeAsHex(byte[] array) {
        return "0x" + PtBytes.encodeHexString(array);
    }

    public static ByteBuffer makeCompositeBuffer(byte[] colName, long positiveTimestamp) {
        assert colName.length <= 1 << 16 : "Cannot use column names larger than 64KiB, was " + colName.length;

        ByteBuffer buffer = ByteBuffer
                .allocate(6 /* misc */ + 8 /* timestamp */ + colName.length)
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
        Validate.isTrue(shouldBeZero == 0);

        byte shouldBe8 = composite.get();
        Validate.isTrue(shouldBe8 == 8);
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
            System.arraycopy(buffer.array(), buffer.position(), bytes, 0, bytes.length);
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
     *
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
        return Cell.create(lowerCaseTableReferenceToBytes(tableRef), "m".getBytes(StandardCharsets.UTF_8));
    }

    @SuppressWarnings("checkstyle:RegexpSinglelineJava")
    static Cell getOldMetadataCell(TableReference tableRef) {
        return Cell.create(
                tableRef.getQualifiedName().getBytes(Charset.defaultCharset()),
                "m".getBytes(StandardCharsets.UTF_8));
    }

    @SuppressWarnings("checkstyle:RegexpSinglelineJava")
    private static byte[] lowerCaseTableReferenceToBytes(TableReference tableRef) {
        return tableRef.getQualifiedName().toLowerCase().getBytes(Charset.defaultCharset());
    }

    @SuppressWarnings("checkstyle:RegexpSinglelineJava")
    static TableReference tableReferenceFromBytes(byte[] name) {
        return TableReference.createUnsafe(new String(name, Charset.defaultCharset()));
    }

    static TableReference tableReferenceFromCfDef(CfDef cf) {
        return TableReference.fromInternalTableName(cf.getName());
    }

    interface ThreadSafeResultVisitor extends Visitor<Map<ByteBuffer, List<ColumnOrSuperColumn>>> {
        // marker
    }

    static class StartTsResultsCollector implements ThreadSafeResultVisitor {
        private final Map<Cell, Value> collectedResults = Maps.newConcurrentMap();
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

    private static void extractTimestampResults(@Output Multimap<Cell, Long> ret,
                                                Map<ByteBuffer, List<ColumnOrSuperColumn>> results) {
        for (Entry<ByteBuffer, List<ColumnOrSuperColumn>> result : results.entrySet()) {
            byte[] row = CassandraKeyValueServices.getBytesFromByteBuffer(result.getKey());
            for (ColumnOrSuperColumn col : result.getValue()) {
                Pair<byte[], Long> pair = CassandraKeyValueServices.decomposeName(col.column);
                ret.put(Cell.create(row, pair.lhSide), pair.rhSide);
            }
        }
    }

    public static boolean isEmptyOrInvalidMetadata(byte[] metadata) {
        if (metadata == null
                || Arrays.equals(metadata, AtlasDbConstants.EMPTY_TABLE_METADATA)
                || Arrays.equals(metadata, AtlasDbConstants.GENERIC_TABLE_METADATA)) {
            return true;
        }
        return false;
    }

}

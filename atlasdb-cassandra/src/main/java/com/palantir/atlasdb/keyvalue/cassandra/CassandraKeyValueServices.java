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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.commons.lang3.Validate;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.common.annotation.Output;
import com.palantir.common.base.Throwables;
import com.palantir.common.visitor.Visitor;
import com.palantir.util.Pair;

public final class CassandraKeyValueServices {
    private static final Logger log = LoggerFactory.getLogger(CassandraKeyValueService.class); // did this on purpose

    private static final long INITIAL_SLEEP_TIME = 100;
    private static final long MAX_SLEEP_TIME = 5000;
    private static final String VERSION_UNREACHABLE = "UNREACHABLE";

    private CassandraKeyValueServices() {
        // Utility class
    }

    static void waitForSchemaVersions(
            CassandraKeyValueServiceConfig config,
            Cassandra.Client client,
            String tableName)
            throws TException {
        waitForSchemaVersions(config, client, tableName, false);
    }

    /**
     * Attempt to wait until nodes' schema versions match.
     *
     * @param config the KVS configuration.
     * @param client Cassandra client.
     * @param tableName table being modified.
     * @param allowQuorumAgreement if true, only a quorum of nodes must agree if the rest of the nodes are unreachable.
     * @throws IllegalStateException if we wait for more than schemaMutationTimeoutMillis specified in config.
     */
    static void waitForSchemaVersions(
            CassandraKeyValueServiceConfig config,
            Cassandra.Client client,
            String tableName,
            boolean allowQuorumAgreement)
            throws TException {
        long start = System.currentTimeMillis();
        long sleepTime = INITIAL_SLEEP_TIME;
        Map<String, List<String>> versions;
        do {
            // This only returns the schema versions of nodes that the client knows exist. In particular, if a node we
            // shook hands with goes down, it will have schema version UNREACHABLE; however, if we never shook hands
            // with a node, there will simply be no entry for it in the map. Hence the check for the number of nodes.
            versions = client.describe_schema_versions();
            if (requiredNumberNodesAgreeOnSchemaVersion(allowQuorumAgreement, config, versions)) {
                return;
            }
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                throw Throwables.throwUncheckedException(e);
            }
            sleepTime = Math.min(sleepTime * 2, MAX_SLEEP_TIME);
        } while (System.currentTimeMillis() < start + config.schemaMutationTimeoutMillis());

        StringBuilder schemaVersions = new StringBuilder();
        for (Entry<String, List<String>> version : versions.entrySet()) {
            schemaVersions.append(String.format("%nAt schema version %s:", version.getKey()));
            for (String node : version.getValue()) {
                schemaVersions.append(String.format("%n\tNode: %s", node));
            }
        }
        String errorMessage = String.format("Cassandra cluster cannot come to agreement on schema versions,"
                        + " after attempting to modify table %s. %s"
                        + " \nFind the nodes above that diverge from the majority schema"
                        + " or have schema 'UNKNOWN', which likely means they are down/unresponsive"
                        + " and examine their logs to determine the issue."
                        + " Fixing the underlying issue and restarting Cassandra should resolve the problem."
                        + " You can quick-check this with 'nodetool describecluster'."
                        + " For this node, the current keyspaces are: %s",
                tableName,
                schemaVersions.toString(),
                client.describe_keyspaces());
        throw new IllegalStateException(errorMessage);
    }

    /**
     * @param allowQuorumAgreement if true, requires only a quorum of nodes to be in agreement. If false, all nodes
     * need to have the same schema version.
     */
    private static boolean requiredNumberNodesAgreeOnSchemaVersion(
            boolean allowQuorumAgreement,
            CassandraKeyValueServiceConfig config,
            Map<String, List<String>> versions) {
        if (getNumberOfReachableSchemas(versions) > 1) {
            return false;
        }

        int numberOfServers = config.servers().size();
        int numberOfVisibleNodes = getNumberOfReachableNodes(versions);

        if (allowQuorumAgreement) {
            return numberOfVisibleNodes >= ((numberOfServers / 2) + 1);
        }
        return numberOfVisibleNodes == numberOfServers;
    }

    private static long getNumberOfReachableSchemas(Map<String, List<String>> versions) {
        return versions.keySet().stream().filter(schema -> !schema.equals(VERSION_UNREACHABLE)).count();
    }

    private static int getNumberOfReachableNodes(Map<String, List<String>> versions) {
        return versions.entrySet().stream().filter(entry -> !entry.getKey().equals(VERSION_UNREACHABLE))
                .map(Entry::getValue).mapToInt(List::size).sum();
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
                waitForSchemaVersions(
                        config,
                        client,
                        "(none, just an initialization check)",
                        true);
                return null;
            });
        } catch (Exception e) {
            log.warn("Failed to retrieve current Cassandra cluster schema status.", e);
        }
    }

    static String encodeAsHex(byte[] array) {
        return "0x" + PtBytes.encodeHexString(array);
    }

    static ByteBuffer makeCompositeBuffer(byte[] colName, long positiveTimestamp) {
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
    static Pair<byte[], Long> decomposeName(Column column) {
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

    static String buildErrorMessage(String prefix, Map<String, Throwable> errorsByHost) {
        StringBuilder result = new StringBuilder();
        result.append(prefix).append("\n\n");
        for (Map.Entry<String, Throwable> entry : errorsByHost.entrySet()) {
            String host = entry.getKey();
            Throwable cause = entry.getValue();
            result.append(String.format("Error on host %s:%n%s%n%n", host, cause));
        }
        return result.toString();
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

    interface ThreadSafeResultVisitor extends Visitor<Map<ByteBuffer, List<ColumnOrSuperColumn>>> {
        // marker
    }

    static class StartTsResultsCollector implements ThreadSafeResultVisitor {
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

    protected static int convertTtl(final long durationMillis, TimeUnit sourceTimeUnit) {
        long ttlSeconds = TimeUnit.SECONDS.convert(durationMillis, sourceTimeUnit);
        Preconditions.checkArgument(ttlSeconds > 0 && ttlSeconds < Integer.MAX_VALUE,
                "Expiration time must be between 0 and ~68 years");
        return (int) ttlSeconds;
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

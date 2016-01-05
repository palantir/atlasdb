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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.commons.lang.Validate;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.common.annotation.Output;
import com.palantir.common.base.Throwables;
import com.palantir.util.Pair;
import com.palantir.util.Visitor;

public class CassandraKeyValueServices {
    private static final Logger log = LoggerFactory.getLogger(CassandraKeyValueService.class); // did this on purpose

    private static long INITIAL_SLEEP_TIME = 100;
    private static long MAX_SLEEP_TIME = 5000;
    static void waitForSchemaVersions(Cassandra.Client client, String tableName) throws InvalidRequestException, TException {
        long start = System.currentTimeMillis();
        long sleepTime = INITIAL_SLEEP_TIME;
        Map<String, List<String>> versions;
        do {
            versions = client.describe_schema_versions();
            if (versions.size() <= 1) {
                return;
            }
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                throw Throwables.throwUncheckedException(e);
            }
            sleepTime = Math.min(sleepTime * 2, MAX_SLEEP_TIME);
        } while (System.currentTimeMillis() < start + CassandraConstants.SECONDS_WAIT_FOR_VERSIONS*1000);
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Cassandra cluster cannot come to agreement on schema versions, after attempting to modify table %s.", tableName));
        for ( Entry<String, List<String>> version : versions.entrySet()) {
            sb.append(String.format("\nAt schema version %s:", version.getKey()));
            for (String node: version.getValue()) {
                sb.append(String.format("\n\tNode: %s", node));
            }
        }
        sb.append("\nFind the nodes above that diverge from the majority schema " +
                "(or have schema 'UNKNOWN', which likely means they are down/unresponsive) " +
                "and examine their logs to determine the issue. Fixing the underlying issue and restarting Cassandra " +
                "should resolve the problem. You can quick-check this with 'nodetool describecluster'.");
        throw new IllegalStateException(sb.toString());
    }

    /**
     * This is a request from pbrown / FDEs; basically it's a pain to do DB surgery to get out of failed patch upgrades, the majority of which requires schema mutations; they would find it preferable to stop before starting the actual patch upgrade / setting APPLYING state.
     */
    static void failQuickInInitializationIfClusterAlreadyInInconsistentState(Cassandra.Client client, boolean safetyDisabled) {
        if (safetyDisabled) {
            log.error("Skipped checking the cassandra cluster during initialization, because safety checks are disabled. Please re-enable safety checks when you are outside of your unusual migration period.");
            return;
        }
        String errorMessage = "While checking the cassandra cluster during initialization, we noticed schema versions could not settle. Failing quickly to avoid getting into harder to fix states (i.e. partially applied patch upgrades, etc). " +
                "This state is in rare cases the correct one to be in; for instance schema versions will be incapable of settling in a cluster of heterogenous Cassandra 1.2/2.0 nodes. If that is the case, disable safety checks in your Cassandra KVS preferences.";
        try {
            waitForSchemaVersions(client, "(none, just an initialization check)");
        } catch (TException e) {
            throw new RuntimeException(errorMessage, e);
        } catch (IllegalStateException e) {
            log.error(errorMessage);
            throw e;
        }
    }

    static ByteBuffer makeCompositeBuffer(byte[] colName, long ts) {
        assert colName.length <= 1 << 16 : "Cannot use column names larger than 64KiB, was " + colName.length;

        ByteBuffer buffer = ByteBuffer.allocate(6 /* misc */ + 8 /* timestamp */ + colName.length).order(ByteOrder.BIG_ENDIAN);

        buffer.put((byte) ((colName.length >> 8) & 0xFF));
        buffer.put((byte) (colName.length & 0xFF));
        buffer.put(colName);
        buffer.put((byte) 0);

        buffer.put((byte) 0);
        buffer.put((byte) (8 & 0xFF));
        buffer.putLong(~ts);
        buffer.put((byte) 0);

        buffer.flip();

        return buffer;
    }

    static Pair<byte[], Long> decompose(ByteBuffer composite) {
        composite = composite.slice().order(ByteOrder.BIG_ENDIAN);

        short len = composite.getShort();
        byte[] colName = new byte[len];
        composite.get(colName);

        short shouldBeZero = composite.getShort();
        Validate.isTrue(shouldBeZero == 0);

        byte shouldBe8 = composite.get();
        Validate.isTrue(shouldBe8 == 8);
        long ts = composite.getLong();

        return Pair.create(colName, (~ts));
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

    static Map<ByteBuffer, List<ColumnOrSuperColumn>> getColsByKey(List<KeySlice> firstPage) {
        Map<ByteBuffer, List<ColumnOrSuperColumn>> ret = Maps.newHashMapWithExpectedSize(firstPage.size());
        for (KeySlice e : firstPage) {
            ret.put(ByteBuffer.wrap(e.getKey()), e.getColumns());
        }
        return ret;
    }

    // /Obviously/ this is long (internal cassandra timestamp) + long (internal cassandra clock sequence and node id)
    static String convertCassandraByteBufferUUIDtoString(ByteBuffer uuid) {
        return new UUID(uuid.getLong(uuid.position()), uuid.getLong(uuid.position() + 8)).toString();
    }

    static String buildErrorMessage(String prefix, Map<String, Throwable> errorsByHost) {
        StringBuilder result = new StringBuilder();
        result.append(prefix).append("\n\n");
        for (Map.Entry<String, Throwable> entry : errorsByHost.entrySet()) {
            String host = entry.getKey();
            Throwable cause = entry.getValue();
            result.append(String.format("Error on host %s:\n%s\n\n", host, cause));
        }
        return result.toString();
    }


    static String getFilteredStackTrace(String filter) {
        Exception e = new Exception();
        StackTraceElement[] stackTrace = e.getStackTrace();
        StringBuilder sb = new StringBuilder();
        for (StackTraceElement element : stackTrace) {
            if (element.getClassName().contains(filter)) {
                sb.append(element.toString()).append("\n");
            }
        }
        return sb.toString();
    }

    static interface ThreadSafeResultVisitor extends Visitor<Map<ByteBuffer, List<ColumnOrSuperColumn>>> {
        // marker
    }

    static class StartTsResultsCollector implements ThreadSafeResultVisitor {
        final Map<Cell, Value> collectedResults = Maps.newConcurrentMap();
        final ValueExtractor extractor = new ValueExtractor(collectedResults);
        final long startTs;

        public StartTsResultsCollector(long startTs) {
            this.startTs = startTs;
        }

        @Override
        public void visit(Map<ByteBuffer, List<ColumnOrSuperColumn>> results) {
            extractor.extractResults(results, startTs, ColumnSelection.all());
        }
    }

    static class AllTimestampsCollector implements ThreadSafeResultVisitor {
        final Multimap<Cell, Long> collectedResults = HashMultimap.create();

        @Override
        public synchronized void visit(Map<ByteBuffer, List<ColumnOrSuperColumn>> results) {
            extractTimestampResults(collectedResults, results);
        }
    }

    static private void extractTimestampResults(@Output Multimap<Cell, Long> ret,
                                                Map<ByteBuffer, List<ColumnOrSuperColumn>> results) {
        for (Entry<ByteBuffer, List<ColumnOrSuperColumn>> result : results.entrySet()) {
            byte[] row = CassandraKeyValueServices.getBytesFromByteBuffer(result.getKey());
            for (ColumnOrSuperColumn col : result.getValue()) {
                Pair<byte[], Long> pair = CassandraKeyValueServices.decomposeName(col.column);
                ret.put(Cell.create(row, pair.lhSide), pair.rhSide);
            }
        }
    }

    static protected int convertTtl(final long durationMillis, TimeUnit sourceTimeUnit) {
        long ttlSeconds = TimeUnit.SECONDS.convert(durationMillis, sourceTimeUnit);
        Preconditions.checkArgument((ttlSeconds > 0 && ttlSeconds < Integer.MAX_VALUE), "Expiration time must be between 0 and ~68 years");
        return (int) ttlSeconds;
    }

    // because unfortunately .equals takes into account if fields with defaults are populated or not
    // also because compression_options after serialization / deserialization comes back as blank for the ones we set 4K chunk on... ?!
    public static final boolean isMatchingCf(CfDef clientSide, CfDef clusterSide) {
        String tableName = clientSide.name;
        if (!Objects.equal(clientSide.compaction_strategy_options, clusterSide.compaction_strategy_options)) {
            log.debug("Found client/server disagreement on compaction strategy options for {}. (client = ({}), server = ({}))",
                    tableName,
                    clientSide.compaction_strategy_options,
                    clusterSide.compaction_strategy_options);
            return false;
        }
        if (clientSide.gc_grace_seconds != clusterSide.gc_grace_seconds) {
            log.debug("Found client/server disagreement on gc_grace_seconds for {}. (client = ({}), server = ({}))",
                    tableName,
                    clientSide.gc_grace_seconds,
                    clusterSide.gc_grace_seconds);
            return false;
        }
        if (clientSide.bloom_filter_fp_chance != clusterSide.bloom_filter_fp_chance) {
            log.debug("Found client/server disagreement on bloom filter false positive chance for {}. (client = ({}), server = ({}))",
                    tableName,
                    clientSide.bloom_filter_fp_chance,
                    clusterSide.bloom_filter_fp_chance);
            return false;
        }
        if (!(clientSide.compression_options.get(CassandraConstants.CFDEF_COMPRESSION_CHUNK_LENGTH_KEY).equals(
                clusterSide.compression_options.get(CassandraConstants.CFDEF_COMPRESSION_CHUNK_LENGTH_KEY)))) {
            log.debug("Found client/server disagreement on compression chunk length for {}. (client = ({}), server = ({}))",
                    tableName,
                    clientSide.compression_options.get(CassandraConstants.CFDEF_COMPRESSION_CHUNK_LENGTH_KEY),
                    clusterSide.compression_options.get(CassandraConstants.CFDEF_COMPRESSION_CHUNK_LENGTH_KEY));
            return false;
        }
        if (!Objects.equal(clientSide.compaction_strategy, clusterSide.compaction_strategy)) {
            log.debug("Found client/server disagreement on compaction_strategy for {}. (client = ({}), server = ({}))",
                    tableName,
                    clientSide.compaction_strategy,
                    clusterSide.compaction_strategy);
            return false;
        }

        return true;
    }

    public static boolean isEmptyOrInvalidMetadata(byte[] metadata) {
        if (metadata == null || Arrays.equals(metadata, AtlasDbConstants.EMPTY_TABLE_METADATA) || Arrays.equals(metadata, AtlasDbConstants.GENERIC_TABLE_METADATA)) {
            return true;
        }
        return false;
    }

}

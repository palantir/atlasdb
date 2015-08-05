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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.commons.lang.Validate;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.primitives.Longs;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.common.annotation.Output;
import com.palantir.common.base.Throwables;
import com.palantir.util.Pair;
import com.palantir.util.Visitor;

public class CassandraKeyValueServices {

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
        for (String schemaVersion: versions.keySet()) {
            sb.append(String.format("\nAt schema version %s:", schemaVersion));
            for (String node: versions.get(schemaVersion)) {
                sb.append(String.format("\n\tNode: %s", node));
            }
        }
        sb.append("\nFind the nodes above that diverge from the majority schema " +
                "(or have schema 'UNKNOWN', which likely means they are down/unresponsive) " +
                "and examine their logs to determine the issue. Fixing the underlying issue and restarting Cassandra " +
                "should resolve the problem. You can quick-check this with 'nodetool describecluster'.");
        throw new IllegalStateException(sb.toString());
    }

    static byte[] makeComposite(byte[] colName, long ts) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(6 /* misc */ + 8 /* timestamp */+ colName.length);

        bos.write((byte) ((colName.length >> 8) & 0xFF));
        bos.write((byte) (colName.length & 0xFF));
        bos.write(colName, 0, colName.length);
        bos.write((byte) 0);

        bos.write((byte) 0);
        bos.write((byte) (8 & 0xFF));
        bos.write(Longs.toByteArray((~ts)), 0, 8);
        bos.write((byte) 0);

        return bos.toByteArray();
    }

    static Pair<byte[], Long> decompose(byte[] composite) {
        ByteArrayInputStream bais = new ByteArrayInputStream(composite);
        int len = (bais.read() & 0xff) << 8;
        len |= bais.read() & 0xff;
        byte[] colName = new byte[len];
        bais.read(colName, 0, len);
        int shouldBeZero = bais.read();
        Validate.isTrue(shouldBeZero == 0);

        shouldBeZero = bais.read();
        Validate.isTrue(shouldBeZero == 0);

        int shouldBe8 = bais.read();
        Validate.isTrue(shouldBe8 == 8);
        byte[] longBytes = new byte[8];
        bais.read(longBytes, 0, 8);
        long ts = Longs.fromByteArray(longBytes);
        return Pair.create(colName, (~ts));
    }


    public static byte[] getBytesFromByteBuffer(ByteBuffer rowName) {
        byte[] row = new byte[rowName.limit() - rowName.position()];
        System.arraycopy(rowName.array(), rowName.position(), row, 0, row.length);
        return row;
    }

    static Map<ByteBuffer, List<ColumnOrSuperColumn>> getColsByKey(List<KeySlice> firstPage) {
        Map<ByteBuffer, List<ColumnOrSuperColumn>> ret = Maps.newHashMap();
        for (KeySlice e : firstPage) {
            ret.put(ByteBuffer.wrap(e.getKey()), e.getColumns());
        }
        return ret;
    }

    // /Obviously/ this is long (internal cassandra timestamp) + long (internal cassandra clock sequence and node id)
    static String convertCassandraByteBufferUUIDtoString(ByteBuffer uuid) {
        return new UUID(uuid.getLong(uuid.position()), uuid.getLong(uuid.position() + 8)).toString();
    }

    static Cassandra.Client getClientInternal(String host, int port, boolean isSsl) throws TTransportException {
        TSocket tSocket = new TSocket(host, port, CassandraConstants.CONNECTION_TIMEOUT_MILLIS);
        tSocket.open();
        tSocket.setTimeout(CassandraConstants.SOCKET_TIMEOUT_MILLIS);
        if (isSsl) {
            boolean success = false;
            try {
                SSLSocketFactory factory = (SSLSocketFactory) SSLSocketFactory.getDefault();
                SSLSocket socket = (SSLSocket) factory.createSocket(tSocket.getSocket(), host, port, true);
                tSocket = new TSocket(socket);
                success = true;
            } catch (IOException e) {
                throw new TTransportException(e);
            } finally {
                if (!success) {
                    tSocket.close();
                }
            }
        }
        TTransport tFramedTransport = new TFramedTransport(tSocket, CassandraConstants.CLIENT_MAX_THRIFT_FRAME_SIZE_BYTES);
        TProtocol protocol = new TBinaryProtocol(tFramedTransport);
        Cassandra.Client client = new Cassandra.Client(protocol);
        return client;
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
        for (ByteBuffer rowName : results.keySet()) {
            byte[] row = CassandraKeyValueServices.getBytesFromByteBuffer(rowName);
            for (ColumnOrSuperColumn c : results.get(rowName)) {
                Pair<byte[], Long> pair = CassandraKeyValueServices.decompose(c.column.getName());
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
        if (!clientSide.compaction_strategy_options.equals(clusterSide.compaction_strategy_options))
            return false;
        if (clientSide.gc_grace_seconds != clusterSide.gc_grace_seconds)
            return false;
        if (clientSide.bloom_filter_fp_chance != clusterSide.bloom_filter_fp_chance)
            return false;

        return true;
    }
}

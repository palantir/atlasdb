package com.palantir.atlasdb.schema.stream.generated;

import com.palantir.atlasdb.stream.StreamStorePersistenceConfigurations;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import javax.annotation.CheckForNull;
import javax.annotation.Generated;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Collections2;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.google.common.io.ByteStreams;
import com.google.common.io.CountingInputStream;
import com.google.common.primitives.Ints;
import com.google.protobuf.ByteString;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.protos.generated.StreamPersistence.Status;
import com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata;
import com.palantir.atlasdb.stream.AbstractPersistentStreamStore;
import com.palantir.atlasdb.stream.BlockConsumingInputStream;
import com.palantir.atlasdb.stream.BlockGetter;
import com.palantir.atlasdb.stream.BlockLoader;
import com.palantir.atlasdb.stream.PersistentStreamStore;
import com.palantir.atlasdb.stream.StreamCleanedException;
import com.palantir.atlasdb.stream.StreamStorePersistenceConfiguration;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.atlasdb.transaction.impl.TxTask;
import com.palantir.common.base.Throwables;
import com.palantir.common.compression.StreamCompression;
import com.palantir.common.io.ConcatenatedInputStream;
import com.palantir.util.AssertUtils;
import com.palantir.util.ByteArrayIOStream;
import com.palantir.util.Pair;
import com.palantir.util.crypto.Sha256Hash;
import com.palantir.util.file.DeleteOnCloseFileInputStream;
import com.palantir.util.file.TempFileUtils;

@Generated("com.palantir.atlasdb.table.description.render.StreamStoreRenderer")
@SuppressWarnings("all")
public final class TestHashComponentsStreamStore extends AbstractPersistentStreamStore {
    public static final int BLOCK_SIZE_IN_BYTES = 1000000; // 1MB. DO NOT CHANGE THIS WITHOUT AN UPGRADE TASK
    public static final int IN_MEMORY_THRESHOLD = 4194304; // streams under this size are kept in memory when loaded
    public static final String STREAM_FILE_PREFIX = "TestHashComponents_stream_";
    public static final String STREAM_FILE_SUFFIX = ".tmp";

    private static final Logger log = LoggerFactory.getLogger(TestHashComponentsStreamStore.class);

    private final StreamTestTableFactory tables;

    private TestHashComponentsStreamStore(TransactionManager txManager, StreamTestTableFactory tables) {
        this(txManager, tables, () -> StreamStorePersistenceConfigurations.DEFAULT_CONFIG);
    }

    private TestHashComponentsStreamStore(TransactionManager txManager, StreamTestTableFactory tables, Supplier<StreamStorePersistenceConfiguration> persistenceConfiguration) {
        super(txManager, StreamCompression.NONE, persistenceConfiguration);
        this.tables = tables;
    }

    public static TestHashComponentsStreamStore of(TransactionManager txManager, StreamTestTableFactory tables) {
        return new TestHashComponentsStreamStore(txManager, tables);
    }

    public static TestHashComponentsStreamStore of(TransactionManager txManager, StreamTestTableFactory tables,  Supplier<StreamStorePersistenceConfiguration> persistenceConfiguration) {
        return new TestHashComponentsStreamStore(txManager, tables, persistenceConfiguration);
    }

    /**
     * This should only be used by test code or as a performance optimization.
     */
    static TestHashComponentsStreamStore of(StreamTestTableFactory tables) {
        return new TestHashComponentsStreamStore(null, tables);
    }

    @Override
    protected long getInMemoryThreshold() {
        return IN_MEMORY_THRESHOLD;
    }

    @Override
    protected void storeBlock(Transaction t, long id, long blockNumber, final byte[] block) {
        Preconditions.checkArgument(block.length <= BLOCK_SIZE_IN_BYTES, "Block to store in DB must be less than BLOCK_SIZE_IN_BYTES");
        final TestHashComponentsStreamValueTable.TestHashComponentsStreamValueRow row = TestHashComponentsStreamValueTable.TestHashComponentsStreamValueRow.of(id, blockNumber);
        try {
            // Do a touch operation on this table to ensure we get a conflict if someone cleans it up.
            touchMetadataWhileStoringForConflicts(t, row.getId(), row.getBlockId());
            tables.getTestHashComponentsStreamValueTable(t).putValue(row, block);
        } catch (RuntimeException e) {
            log.error("Error storing block {} for stream id {}", row.getBlockId(), row.getId(), e);
            throw e;
        }
    }

    private void touchMetadataWhileStoringForConflicts(Transaction t, Long id, long blockNumber) {
        TestHashComponentsStreamMetadataTable metaTable = tables.getTestHashComponentsStreamMetadataTable(t);
        TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow row = TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow.of(id);
        StreamMetadata metadata = metaTable.getMetadatas(ImmutableSet.of(row)).values().iterator().next();
        Preconditions.checkState(metadata.getStatus() == Status.STORING, "This stream is being cleaned up while storing blocks: %s", id);
        StreamMetadata.Builder builder = StreamMetadata.newBuilder(metadata);
        builder.setLength(blockNumber * BLOCK_SIZE_IN_BYTES + 1);
        metaTable.putMetadata(row, builder.build());
    }

    @Override
    protected void putMetadataAndHashIndexTask(Transaction t, Map<Long, StreamMetadata> streamIdsToMetadata) {
        TestHashComponentsStreamMetadataTable mdTable = tables.getTestHashComponentsStreamMetadataTable(t);
        Map<Long, StreamMetadata> prevMetadatas = getMetadata(t, streamIdsToMetadata.keySet());

        Map<TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow, StreamMetadata> rowsToStoredMetadata = new HashMap<>();
        Map<TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow, StreamMetadata> rowsToUnstoredMetadata = new HashMap<>();
        for (Entry<Long, StreamMetadata> e : streamIdsToMetadata.entrySet()) {
            long streamId = e.getKey();
            StreamMetadata metadata = e.getValue();
            StreamMetadata prevMetadata = prevMetadatas.get(streamId);
            if (metadata.getStatus() == Status.STORED) {
                if (prevMetadata == null || prevMetadata.getStatus() != Status.STORING) {
                    // This can happen if we cleanup old streams.
                    throw new TransactionFailedRetriableException("Cannot mark a stream as stored that isn't currently storing: " + prevMetadata);
                }
                rowsToStoredMetadata.put(TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow.of(streamId), metadata);
            } else if (metadata.getStatus() == Status.STORING) {
                // This will prevent two users trying to store the same id.
                if (prevMetadata != null) {
                    throw new TransactionFailedRetriableException("Cannot reuse the same stream id: " + streamId);
                }
                rowsToUnstoredMetadata.put(TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow.of(streamId), metadata);
            }
        }
        putHashIndexTask(t, rowsToStoredMetadata);

        Map<TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow, StreamMetadata> rowsToMetadata = new HashMap<>();
        rowsToMetadata.putAll(rowsToStoredMetadata);
        rowsToMetadata.putAll(rowsToUnstoredMetadata);
        mdTable.putMetadata(rowsToMetadata);
    }

    private long getNumberOfBlocksFromMetadata(StreamMetadata metadata) {
        return (metadata.getLength() + BLOCK_SIZE_IN_BYTES - 1) / BLOCK_SIZE_IN_BYTES;
    }

    @Override
    protected File createTempFile(Long id) throws IOException {
        File file = TempFileUtils.createTempFile(STREAM_FILE_PREFIX + id, STREAM_FILE_SUFFIX);
        file.deleteOnExit();
        return file;
    }

    @Override
    protected void loadSingleBlockToOutputStream(Transaction t, Long streamId, long blockId, OutputStream os) {
        TestHashComponentsStreamValueTable.TestHashComponentsStreamValueRow row = TestHashComponentsStreamValueTable.TestHashComponentsStreamValueRow.of(streamId, blockId);
        try {
            os.write(getBlock(t, row));
        } catch (RuntimeException e) {
            log.error("Error storing block {} for stream id {}", row.getBlockId(), row.getId(), e);
            throw e;
        } catch (IOException e) {
            log.error("Error writing block {} to file when getting stream id {}", row.getBlockId(), row.getId(), e);
            throw Throwables.rewrapAndThrowUncheckedException("Error writing blocks to file when creating stream.", e);
        }
    }

    private byte[] getBlock(Transaction t, TestHashComponentsStreamValueTable.TestHashComponentsStreamValueRow row) {
        TestHashComponentsStreamValueTable valueTable = tables.getTestHashComponentsStreamValueTable(t);
        return valueTable.getValues(ImmutableSet.of(row)).get(row);
    }

    @Override
    protected Map<Long, StreamMetadata> getMetadata(Transaction t, Set<Long> streamIds) {
        if (streamIds.isEmpty()) {
            return ImmutableMap.of();
        }
        TestHashComponentsStreamMetadataTable table = tables.getTestHashComponentsStreamMetadataTable(t);
        Map<TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow, StreamMetadata> metadatas = table.getMetadatas(getMetadataRowsForIds(streamIds));
        Map<Long, StreamMetadata> ret = new HashMap<>();
        for (Map.Entry<TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow, StreamMetadata> e : metadatas.entrySet()) {
            ret.put(e.getKey().getId(), e.getValue());
        }
        return ret;
    }

    @Override
    public Map<Sha256Hash, Long> lookupStreamIdsByHash(Transaction t, final Set<Sha256Hash> hashes) {
        if (hashes.isEmpty()) {
            return ImmutableMap.of();
        }
        TestHashComponentsStreamHashAidxTable idx = tables.getTestHashComponentsStreamHashAidxTable(t);
        Set<TestHashComponentsStreamHashAidxTable.TestHashComponentsStreamHashAidxRow> rows = getHashIndexRowsForHashes(hashes);

        Multimap<TestHashComponentsStreamHashAidxTable.TestHashComponentsStreamHashAidxRow, TestHashComponentsStreamHashAidxTable.TestHashComponentsStreamHashAidxColumnValue> m = idx.getRowsMultimap(rows);
        Map<Long, Sha256Hash> hashForStreams = new HashMap<>();
        for (TestHashComponentsStreamHashAidxTable.TestHashComponentsStreamHashAidxRow r : m.keySet()) {
            for (TestHashComponentsStreamHashAidxTable.TestHashComponentsStreamHashAidxColumnValue v : m.get(r)) {
                Long streamId = v.getColumnName().getStreamId();
                Sha256Hash hash = r.getHash();
                if (hashForStreams.containsKey(streamId)) {
                    AssertUtils.assertAndLog(log, hashForStreams.get(streamId).equals(hash), "(BUG) Stream ID has 2 different hashes: " + streamId);
                }
                hashForStreams.put(streamId, hash);
            }
        }
        Map<Long, StreamMetadata> metadata = getMetadata(t, hashForStreams.keySet());

        Map<Sha256Hash, Long> ret = new HashMap<>();
        for (Map.Entry<Long, StreamMetadata> e : metadata.entrySet()) {
            if (e.getValue().getStatus() != Status.STORED) {
                continue;
            }
            Sha256Hash hash = hashForStreams.get(e.getKey());
            ret.put(hash, e.getKey());
        }

        return ret;
    }

    private Set<TestHashComponentsStreamHashAidxTable.TestHashComponentsStreamHashAidxRow> getHashIndexRowsForHashes(final Set<Sha256Hash> hashes) {
        Set<TestHashComponentsStreamHashAidxTable.TestHashComponentsStreamHashAidxRow> rows = new HashSet<>();
        for (Sha256Hash h : hashes) {
            rows.add(TestHashComponentsStreamHashAidxTable.TestHashComponentsStreamHashAidxRow.of(h));
        }
        return rows;
    }

    private Set<TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow> getMetadataRowsForIds(final Iterable<Long> ids) {
        Set<TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow> rows = new HashSet<>();
        for (Long id : ids) {
            rows.add(TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow.of(id));
        }
        return rows;
    }

    private void putHashIndexTask(Transaction t, Map<TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow, StreamMetadata> rowsToMetadata) {
        Multimap<TestHashComponentsStreamHashAidxTable.TestHashComponentsStreamHashAidxRow, TestHashComponentsStreamHashAidxTable.TestHashComponentsStreamHashAidxColumnValue> indexMap = HashMultimap.create();
        for (Entry<TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow, StreamMetadata> e : rowsToMetadata.entrySet()) {
            TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow row = e.getKey();
            StreamMetadata metadata = e.getValue();
            Preconditions.checkArgument(
                    metadata.getStatus() == Status.STORED,
                    "Should only index successfully stored streams.");

            Sha256Hash hash = Sha256Hash.EMPTY;
            if (!ByteString.EMPTY.equals(metadata.getHash())) {
                hash = new Sha256Hash(metadata.getHash().toByteArray());
            }
            TestHashComponentsStreamHashAidxTable.TestHashComponentsStreamHashAidxRow hashRow = TestHashComponentsStreamHashAidxTable.TestHashComponentsStreamHashAidxRow.of(hash);
            TestHashComponentsStreamHashAidxTable.TestHashComponentsStreamHashAidxColumn column = TestHashComponentsStreamHashAidxTable.TestHashComponentsStreamHashAidxColumn.of(row.getId());
            TestHashComponentsStreamHashAidxTable.TestHashComponentsStreamHashAidxColumnValue columnValue = TestHashComponentsStreamHashAidxTable.TestHashComponentsStreamHashAidxColumnValue.of(column, 0L);
            indexMap.put(hashRow, columnValue);
        }
        TestHashComponentsStreamHashAidxTable hiTable = tables.getTestHashComponentsStreamHashAidxTable(t);
        hiTable.put(indexMap);
    }

    /**
     * This should only be used from the cleanup tasks.
     */
    void deleteStreams(Transaction t, final Set<Long> streamIds) {
        if (streamIds.isEmpty()) {
            return;
        }
        Set<TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow> smRows = new HashSet<>();
        Multimap<TestHashComponentsStreamHashAidxTable.TestHashComponentsStreamHashAidxRow, TestHashComponentsStreamHashAidxTable.TestHashComponentsStreamHashAidxColumn> shToDelete = HashMultimap.create();
        for (Long streamId : streamIds) {
            smRows.add(TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow.of(streamId));
        }
        TestHashComponentsStreamMetadataTable table = tables.getTestHashComponentsStreamMetadataTable(t);
        Map<TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow, StreamMetadata> metadatas = table.getMetadatas(smRows);
        Set<TestHashComponentsStreamValueTable.TestHashComponentsStreamValueRow> streamValueToDelete = new HashSet<>();
        for (Entry<TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow, StreamMetadata> e : metadatas.entrySet()) {
            Long streamId = e.getKey().getId();
            long blocks = getNumberOfBlocksFromMetadata(e.getValue());
            for (long i = 0; i < blocks; i++) {
                streamValueToDelete.add(TestHashComponentsStreamValueTable.TestHashComponentsStreamValueRow.of(streamId, i));
            }
            ByteString streamHash = e.getValue().getHash();
            Sha256Hash hash = Sha256Hash.EMPTY;
            if (!ByteString.EMPTY.equals(streamHash)) {
                hash = new Sha256Hash(streamHash.toByteArray());
            } else {
                log.error("Empty hash for stream {}", streamId);
            }
            TestHashComponentsStreamHashAidxTable.TestHashComponentsStreamHashAidxRow hashRow = TestHashComponentsStreamHashAidxTable.TestHashComponentsStreamHashAidxRow.of(hash);
            TestHashComponentsStreamHashAidxTable.TestHashComponentsStreamHashAidxColumn column = TestHashComponentsStreamHashAidxTable.TestHashComponentsStreamHashAidxColumn.of(streamId);
            shToDelete.put(hashRow, column);
        }
        tables.getTestHashComponentsStreamHashAidxTable(t).delete(shToDelete);
        tables.getTestHashComponentsStreamValueTable(t).delete(streamValueToDelete);
        table.delete(smRows);
    }

    @Override
    protected void markStreamsAsUsedInternal(Transaction t, final Map<Long, byte[]> streamIdsToReference) {
        if (streamIdsToReference.isEmpty()) {
            return;
        }
        TestHashComponentsStreamIdxTable index = tables.getTestHashComponentsStreamIdxTable(t);
        Multimap<TestHashComponentsStreamIdxTable.TestHashComponentsStreamIdxRow, TestHashComponentsStreamIdxTable.TestHashComponentsStreamIdxColumnValue> rowsToValues = HashMultimap.create();
        for (Map.Entry<Long, byte[]> entry : streamIdsToReference.entrySet()) {
            Long streamId = entry.getKey();
            byte[] reference = entry.getValue();
            TestHashComponentsStreamIdxTable.TestHashComponentsStreamIdxColumn col = TestHashComponentsStreamIdxTable.TestHashComponentsStreamIdxColumn.of(reference);
            TestHashComponentsStreamIdxTable.TestHashComponentsStreamIdxColumnValue value = TestHashComponentsStreamIdxTable.TestHashComponentsStreamIdxColumnValue.of(col, 0L);
            rowsToValues.put(TestHashComponentsStreamIdxTable.TestHashComponentsStreamIdxRow.of(streamId), value);
        }
        index.put(rowsToValues);
    }

    @Override
    public void unmarkStreamsAsUsed(Transaction t, final Map<Long, byte[]> streamIdsToReference) {
        if (streamIdsToReference.isEmpty()) {
            return;
        }
        TestHashComponentsStreamIdxTable index = tables.getTestHashComponentsStreamIdxTable(t);
        Multimap<TestHashComponentsStreamIdxTable.TestHashComponentsStreamIdxRow, TestHashComponentsStreamIdxTable.TestHashComponentsStreamIdxColumn> toDelete = ArrayListMultimap.create(streamIdsToReference.size(), 1);
        for (Map.Entry<Long, byte[]> entry : streamIdsToReference.entrySet()) {
            Long streamId = entry.getKey();
            byte[] reference = entry.getValue();
            TestHashComponentsStreamIdxTable.TestHashComponentsStreamIdxColumn col = TestHashComponentsStreamIdxTable.TestHashComponentsStreamIdxColumn.of(reference);
            toDelete.put(TestHashComponentsStreamIdxTable.TestHashComponentsStreamIdxRow.of(streamId), col);
        }
        index.delete(toDelete);
    }

    @Override
    protected void touchMetadataWhileMarkingUsedForConflicts(Transaction t, Iterable<Long> ids) {
        TestHashComponentsStreamMetadataTable metaTable = tables.getTestHashComponentsStreamMetadataTable(t);
        Set<TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow> rows = new HashSet<>();
        for (Long id : ids) {
            rows.add(TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow.of(id));
        }
        Map<TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow, StreamMetadata> metadatas = metaTable.getMetadatas(rows);
        for (Map.Entry<TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow, StreamMetadata> e : metadatas.entrySet()) {
            StreamMetadata metadata = e.getValue();
            Preconditions.checkState(metadata.getStatus() == Status.STORED,
            "Stream: %s has status: %s", e.getKey().getId(), metadata.getStatus());
            metaTable.putMetadata(e.getKey(), metadata);
        }
        SetView<TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow> missingRows = Sets.difference(rows, metadatas.keySet());
        if (!missingRows.isEmpty()) {
            throw new IllegalStateException("Missing metadata rows for:" + missingRows
            + " rows: " + rows + " metadata: " + metadatas + " txn timestamp: " + t.getTimestamp());
        }
    }

    /**
     * This exists to avoid unused import warnings
     * {@link AbstractPersistentStreamStore}
     * {@link ArrayListMultimap}
     * {@link Arrays}
     * {@link AssertUtils}
     * {@link BiConsumer}
     * {@link BlockConsumingInputStream}
     * {@link BlockGetter}
     * {@link BlockLoader}
     * {@link BufferedInputStream}
     * {@link ByteArrayIOStream}
     * {@link ByteArrayInputStream}
     * {@link ByteStreams}
     * {@link ByteString}
     * {@link Cell}
     * {@link CheckForNull}
     * {@link Collection}
     * {@link Collections2}
     * {@link ConcatenatedInputStream}
     * {@link CountingInputStream}
     * {@link DeleteOnCloseFileInputStream}
     * {@link DigestInputStream}
     * {@link Entry}
     * {@link File}
     * {@link FileNotFoundException}
     * {@link FileOutputStream}
     * {@link Functions}
     * {@link Generated}
     * {@link HashMap}
     * {@link HashMultimap}
     * {@link HashSet}
     * {@link IOException}
     * {@link ImmutableMap}
     * {@link ImmutableSet}
     * {@link InputStream}
     * {@link Ints}
     * {@link List}
     * {@link Lists}
     * {@link Logger}
     * {@link LoggerFactory}
     * {@link Map}
     * {@link Maps}
     * {@link MessageDigest}
     * {@link Multimap}
     * {@link Multimaps}
     * {@link Optional}
     * {@link OutputStream}
     * {@link Pair}
     * {@link PersistentStreamStore}
     * {@link Preconditions}
     * {@link Set}
     * {@link SetView}
     * {@link Sets}
     * {@link Sha256Hash}
     * {@link Status}
     * {@link StreamCleanedException}
     * {@link StreamCompression}
     * {@link StreamMetadata}
     * {@link StreamStorePersistenceConfiguration}
     * {@link Supplier}
     * {@link TempFileUtils}
     * {@link Throwables}
     * {@link TimeUnit}
     * {@link Transaction}
     * {@link TransactionFailedRetriableException}
     * {@link TransactionManager}
     * {@link TransactionTask}
     * {@link TxTask}
     */
    static final int dummy = 0;
}
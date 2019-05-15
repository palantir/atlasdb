package com.palantir.atlasdb.blob.generated;

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
import com.palantir.common.compression.LZ4CompressingInputStream;
import com.palantir.common.io.ConcatenatedInputStream;
import com.palantir.util.AssertUtils;
import com.palantir.util.ByteArrayIOStream;
import com.palantir.util.Pair;
import com.palantir.util.crypto.Sha256Hash;
import com.palantir.util.file.DeleteOnCloseFileInputStream;
import com.palantir.util.file.TempFileUtils;

import net.jpountz.lz4.LZ4BlockInputStream;

@Generated("com.palantir.atlasdb.table.description.render.StreamStoreRenderer")
@SuppressWarnings("all")
public final class DataStreamStore extends AbstractPersistentStreamStore {
    public static final int BLOCK_SIZE_IN_BYTES = 1000000; // 1MB. DO NOT CHANGE THIS WITHOUT AN UPGRADE TASK
    public static final int IN_MEMORY_THRESHOLD = 4194304; // streams under this size are kept in memory when loaded
    public static final String STREAM_FILE_PREFIX = "Data_stream_";
    public static final String STREAM_FILE_SUFFIX = ".tmp";

    private static final Logger log = LoggerFactory.getLogger(DataStreamStore.class);

    private final BlobSchemaTableFactory tables;

    private DataStreamStore(TransactionManager txManager, BlobSchemaTableFactory tables) {
        this(txManager, tables, () -> StreamStorePersistenceConfiguration.DEFAULT_CONFIG);
    }

    private DataStreamStore(TransactionManager txManager, BlobSchemaTableFactory tables, Supplier<StreamStorePersistenceConfiguration> persistenceConfiguration) {
        super(txManager, persistenceConfiguration);
        this.tables = tables;
    }

    public static DataStreamStore of(TransactionManager txManager, BlobSchemaTableFactory tables) {
        return new DataStreamStore(txManager, tables);
    }

    public static DataStreamStore of(TransactionManager txManager, BlobSchemaTableFactory tables,  Supplier<StreamStorePersistenceConfiguration> persistenceConfiguration) {
        return new DataStreamStore(txManager, tables, persistenceConfiguration);
    }

    /**
     * This should only be used by test code or as a performance optimization.
     */
    static DataStreamStore of(BlobSchemaTableFactory tables) {
        return new DataStreamStore(null, tables);
    }

    @Override
    protected long getInMemoryThreshold() {
        return IN_MEMORY_THRESHOLD;
    }

    @Override
    protected void storeBlock(Transaction t, long id, long blockNumber, final byte[] block) {
        Preconditions.checkArgument(block.length <= BLOCK_SIZE_IN_BYTES, "Block to store in DB must be less than BLOCK_SIZE_IN_BYTES");
        final DataStreamValueTable.DataStreamValueRow row = DataStreamValueTable.DataStreamValueRow.of(id, blockNumber);
        try {
            // Do a touch operation on this table to ensure we get a conflict if someone cleans it up.
            touchMetadataWhileStoringForConflicts(t, row.getId(), row.getBlockId());
            tables.getDataStreamValueTable(t).putValue(row, block);
        } catch (RuntimeException e) {
            log.error("Error storing block {} for stream id {}", row.getBlockId(), row.getId(), e);
            throw e;
        }
    }

    private void touchMetadataWhileStoringForConflicts(Transaction t, Long id, long blockNumber) {
        DataStreamMetadataTable metaTable = tables.getDataStreamMetadataTable(t);
        DataStreamMetadataTable.DataStreamMetadataRow row = DataStreamMetadataTable.DataStreamMetadataRow.of(id);
        StreamMetadata metadata = metaTable.getMetadatas(ImmutableSet.of(row)).values().iterator().next();
        Preconditions.checkState(metadata.getStatus() == Status.STORING, "This stream is being cleaned up while storing blocks: %s", id);
        StreamMetadata.Builder builder = StreamMetadata.newBuilder(metadata);
        builder.setLength(blockNumber * BLOCK_SIZE_IN_BYTES + 1);
        metaTable.putMetadata(row, builder.build());
    }

    @Override
    protected void putMetadataAndHashIndexTask(Transaction t, Map<Long, StreamMetadata> streamIdsToMetadata) {
        DataStreamMetadataTable mdTable = tables.getDataStreamMetadataTable(t);
        Map<Long, StreamMetadata> prevMetadatas = getMetadata(t, streamIdsToMetadata.keySet());

        Map<DataStreamMetadataTable.DataStreamMetadataRow, StreamMetadata> rowsToStoredMetadata = Maps.newHashMap();
        Map<DataStreamMetadataTable.DataStreamMetadataRow, StreamMetadata> rowsToUnstoredMetadata = Maps.newHashMap();
        for (Entry<Long, StreamMetadata> e : streamIdsToMetadata.entrySet()) {
            long streamId = e.getKey();
            StreamMetadata metadata = e.getValue();
            StreamMetadata prevMetadata = prevMetadatas.get(streamId);
            if (metadata.getStatus() == Status.STORED) {
                if (prevMetadata == null || prevMetadata.getStatus() != Status.STORING) {
                    // This can happen if we cleanup old streams.
                    throw new TransactionFailedRetriableException("Cannot mark a stream as stored that isn't currently storing: " + prevMetadata);
                }
                rowsToStoredMetadata.put(DataStreamMetadataTable.DataStreamMetadataRow.of(streamId), metadata);
            } else if (metadata.getStatus() == Status.STORING) {
                // This will prevent two users trying to store the same id.
                if (prevMetadata != null) {
                    throw new TransactionFailedRetriableException("Cannot reuse the same stream id: " + streamId);
                }
                rowsToUnstoredMetadata.put(DataStreamMetadataTable.DataStreamMetadataRow.of(streamId), metadata);
            }
        }
        putHashIndexTask(t, rowsToStoredMetadata);

        Map<DataStreamMetadataTable.DataStreamMetadataRow, StreamMetadata> rowsToMetadata = Maps.newHashMap();
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
        DataStreamValueTable.DataStreamValueRow row = DataStreamValueTable.DataStreamValueRow.of(streamId, blockId);
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

    private byte[] getBlock(Transaction t, DataStreamValueTable.DataStreamValueRow row) {
        DataStreamValueTable valueTable = tables.getDataStreamValueTable(t);
        return valueTable.getValues(ImmutableSet.of(row)).get(row);
    }

    @Override
    protected Map<Long, StreamMetadata> getMetadata(Transaction t, Set<Long> streamIds) {
        if (streamIds.isEmpty()) {
            return ImmutableMap.of();
        }
        DataStreamMetadataTable table = tables.getDataStreamMetadataTable(t);
        Map<DataStreamMetadataTable.DataStreamMetadataRow, StreamMetadata> metadatas = table.getMetadatas(getMetadataRowsForIds(streamIds));
        Map<Long, StreamMetadata> ret = Maps.newHashMap();
        for (Map.Entry<DataStreamMetadataTable.DataStreamMetadataRow, StreamMetadata> e : metadatas.entrySet()) {
            ret.put(e.getKey().getId(), e.getValue());
        }
        return ret;
    }

    @Override
    public Map<Sha256Hash, Long> lookupStreamIdsByHash(Transaction t, final Set<Sha256Hash> hashes) {
        if (hashes.isEmpty()) {
            return ImmutableMap.of();
        }
        DataStreamHashAidxTable idx = tables.getDataStreamHashAidxTable(t);
        Set<DataStreamHashAidxTable.DataStreamHashAidxRow> rows = getHashIndexRowsForHashes(hashes);

        Multimap<DataStreamHashAidxTable.DataStreamHashAidxRow, DataStreamHashAidxTable.DataStreamHashAidxColumnValue> m = idx.getRowsMultimap(rows);
        Map<Long, Sha256Hash> hashForStreams = Maps.newHashMap();
        for (DataStreamHashAidxTable.DataStreamHashAidxRow r : m.keySet()) {
            for (DataStreamHashAidxTable.DataStreamHashAidxColumnValue v : m.get(r)) {
                Long streamId = v.getColumnName().getStreamId();
                Sha256Hash hash = r.getHash();
                if (hashForStreams.containsKey(streamId)) {
                    AssertUtils.assertAndLog(log, hashForStreams.get(streamId).equals(hash), "(BUG) Stream ID has 2 different hashes: " + streamId);
                }
                hashForStreams.put(streamId, hash);
            }
        }
        Map<Long, StreamMetadata> metadata = getMetadata(t, hashForStreams.keySet());

        Map<Sha256Hash, Long> ret = Maps.newHashMap();
        for (Map.Entry<Long, StreamMetadata> e : metadata.entrySet()) {
            if (e.getValue().getStatus() != Status.STORED) {
                continue;
            }
            Sha256Hash hash = hashForStreams.get(e.getKey());
            ret.put(hash, e.getKey());
        }

        return ret;
    }

    private Set<DataStreamHashAidxTable.DataStreamHashAidxRow> getHashIndexRowsForHashes(final Set<Sha256Hash> hashes) {
        Set<DataStreamHashAidxTable.DataStreamHashAidxRow> rows = Sets.newHashSet();
        for (Sha256Hash h : hashes) {
            rows.add(DataStreamHashAidxTable.DataStreamHashAidxRow.of(h));
        }
        return rows;
    }

    private Set<DataStreamMetadataTable.DataStreamMetadataRow> getMetadataRowsForIds(final Iterable<Long> ids) {
        Set<DataStreamMetadataTable.DataStreamMetadataRow> rows = Sets.newHashSet();
        for (Long id : ids) {
            rows.add(DataStreamMetadataTable.DataStreamMetadataRow.of(id));
        }
        return rows;
    }

    private void putHashIndexTask(Transaction t, Map<DataStreamMetadataTable.DataStreamMetadataRow, StreamMetadata> rowsToMetadata) {
        Multimap<DataStreamHashAidxTable.DataStreamHashAidxRow, DataStreamHashAidxTable.DataStreamHashAidxColumnValue> indexMap = HashMultimap.create();
        for (Entry<DataStreamMetadataTable.DataStreamMetadataRow, StreamMetadata> e : rowsToMetadata.entrySet()) {
            DataStreamMetadataTable.DataStreamMetadataRow row = e.getKey();
            StreamMetadata metadata = e.getValue();
            Preconditions.checkArgument(
                    metadata.getStatus() == Status.STORED,
                    "Should only index successfully stored streams.");

            Sha256Hash hash = Sha256Hash.EMPTY;
            if (metadata.getHash() != com.google.protobuf.ByteString.EMPTY) {
                hash = new Sha256Hash(metadata.getHash().toByteArray());
            }
            DataStreamHashAidxTable.DataStreamHashAidxRow hashRow = DataStreamHashAidxTable.DataStreamHashAidxRow.of(hash);
            DataStreamHashAidxTable.DataStreamHashAidxColumn column = DataStreamHashAidxTable.DataStreamHashAidxColumn.of(row.getId());
            DataStreamHashAidxTable.DataStreamHashAidxColumnValue columnValue = DataStreamHashAidxTable.DataStreamHashAidxColumnValue.of(column, 0L);
            indexMap.put(hashRow, columnValue);
        }
        DataStreamHashAidxTable hiTable = tables.getDataStreamHashAidxTable(t);
        hiTable.put(indexMap);
    }

    /**
     * This should only be used from the cleanup tasks.
     */
    void deleteStreams(Transaction t, final Set<Long> streamIds) {
        if (streamIds.isEmpty()) {
            return;
        }
        Set<DataStreamMetadataTable.DataStreamMetadataRow> smRows = Sets.newHashSet();
        Multimap<DataStreamHashAidxTable.DataStreamHashAidxRow, DataStreamHashAidxTable.DataStreamHashAidxColumn> shToDelete = HashMultimap.create();
        for (Long streamId : streamIds) {
            smRows.add(DataStreamMetadataTable.DataStreamMetadataRow.of(streamId));
        }
        DataStreamMetadataTable table = tables.getDataStreamMetadataTable(t);
        Map<DataStreamMetadataTable.DataStreamMetadataRow, StreamMetadata> metadatas = table.getMetadatas(smRows);
        Set<DataStreamValueTable.DataStreamValueRow> streamValueToDelete = Sets.newHashSet();
        for (Entry<DataStreamMetadataTable.DataStreamMetadataRow, StreamMetadata> e : metadatas.entrySet()) {
            Long streamId = e.getKey().getId();
            long blocks = getNumberOfBlocksFromMetadata(e.getValue());
            for (long i = 0; i < blocks; i++) {
                streamValueToDelete.add(DataStreamValueTable.DataStreamValueRow.of(streamId, i));
            }
            ByteString streamHash = e.getValue().getHash();
            Sha256Hash hash = Sha256Hash.EMPTY;
            if (streamHash != com.google.protobuf.ByteString.EMPTY) {
                hash = new Sha256Hash(streamHash.toByteArray());
            } else {
                log.error("Empty hash for stream {}", streamId);
            }
            DataStreamHashAidxTable.DataStreamHashAidxRow hashRow = DataStreamHashAidxTable.DataStreamHashAidxRow.of(hash);
            DataStreamHashAidxTable.DataStreamHashAidxColumn column = DataStreamHashAidxTable.DataStreamHashAidxColumn.of(streamId);
            shToDelete.put(hashRow, column);
        }
        tables.getDataStreamHashAidxTable(t).delete(shToDelete);
        tables.getDataStreamValueTable(t).delete(streamValueToDelete);
        table.delete(smRows);
    }

    @Override
    protected void markStreamsAsUsedInternal(Transaction t, final Map<Long, byte[]> streamIdsToReference) {
        if (streamIdsToReference.isEmpty()) {
            return;
        }
        DataStreamIdxTable index = tables.getDataStreamIdxTable(t);
        Multimap<DataStreamIdxTable.DataStreamIdxRow, DataStreamIdxTable.DataStreamIdxColumnValue> rowsToValues = HashMultimap.create();
        for (Map.Entry<Long, byte[]> entry : streamIdsToReference.entrySet()) {
            Long streamId = entry.getKey();
            byte[] reference = entry.getValue();
            DataStreamIdxTable.DataStreamIdxColumn col = DataStreamIdxTable.DataStreamIdxColumn.of(reference);
            DataStreamIdxTable.DataStreamIdxColumnValue value = DataStreamIdxTable.DataStreamIdxColumnValue.of(col, 0L);
            rowsToValues.put(DataStreamIdxTable.DataStreamIdxRow.of(streamId), value);
        }
        index.put(rowsToValues);
    }

    @Override
    public void unmarkStreamsAsUsed(Transaction t, final Map<Long, byte[]> streamIdsToReference) {
        if (streamIdsToReference.isEmpty()) {
            return;
        }
        DataStreamIdxTable index = tables.getDataStreamIdxTable(t);
        Multimap<DataStreamIdxTable.DataStreamIdxRow, DataStreamIdxTable.DataStreamIdxColumn> toDelete = ArrayListMultimap.create(streamIdsToReference.size(), 1);
        for (Map.Entry<Long, byte[]> entry : streamIdsToReference.entrySet()) {
            Long streamId = entry.getKey();
            byte[] reference = entry.getValue();
            DataStreamIdxTable.DataStreamIdxColumn col = DataStreamIdxTable.DataStreamIdxColumn.of(reference);
            toDelete.put(DataStreamIdxTable.DataStreamIdxRow.of(streamId), col);
        }
        index.delete(toDelete);
    }

    @Override
    protected void touchMetadataWhileMarkingUsedForConflicts(Transaction t, Iterable<Long> ids) {
        DataStreamMetadataTable metaTable = tables.getDataStreamMetadataTable(t);
        Set<DataStreamMetadataTable.DataStreamMetadataRow> rows = Sets.newHashSet();
        for (Long id : ids) {
            rows.add(DataStreamMetadataTable.DataStreamMetadataRow.of(id));
        }
        Map<DataStreamMetadataTable.DataStreamMetadataRow, StreamMetadata> metadatas = metaTable.getMetadatas(rows);
        for (Map.Entry<DataStreamMetadataTable.DataStreamMetadataRow, StreamMetadata> e : metadatas.entrySet()) {
            StreamMetadata metadata = e.getValue();
            Preconditions.checkState(metadata.getStatus() == Status.STORED,
            "Stream: %s has status: %s", e.getKey().getId(), metadata.getStatus());
            metaTable.putMetadata(e.getKey(), metadata);
        }
        SetView<DataStreamMetadataTable.DataStreamMetadataRow> missingRows = Sets.difference(rows, metadatas.keySet());
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
     * {@link HashMultimap}
     * {@link IOException}
     * {@link ImmutableMap}
     * {@link ImmutableSet}
     * {@link InputStream}
     * {@link Ints}
     * {@link LZ4BlockInputStream}
     * {@link LZ4CompressingInputStream}
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
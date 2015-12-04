package com.palantir.atlasdb.schema.stream.generated;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import com.palantir.atlasdb.protos.generated.StreamPersistence.Status;
import com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata;
import com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata.Builder;
import com.palantir.atlasdb.stream.AbstractExpiringStreamStore;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.common.base.Throwables;
import com.palantir.util.AssertUtils;
import com.palantir.util.crypto.Sha256Hash;
import com.palantir.util.file.TempFileUtils;


public final class StreamTest2StreamStore extends AbstractExpiringStreamStore<Long> {
    public static final int BLOCK_SIZE_IN_BYTES = 1000000; // 1MB. DO NOT CHANGE THIS WITHOUT AN UPGRADE TASK
    public static final int IN_MEMORY_THRESHOLD = 4000; // streams under this size are kept in memory when loaded
    public static final String STREAM_FILE_PREFIX = "StreamTest2_stream_";
    public static final String STREAM_FILE_SUFFIX = ".tmp";

    private static final Logger log = LoggerFactory.getLogger(StreamTest2StreamStore.class);

    private final StreamTestTableFactory tables;

    private StreamTest2StreamStore(TransactionManager txManager, StreamTestTableFactory tables) {
        super(txManager);
        this.tables = tables;
    }

    public static StreamTest2StreamStore of(TransactionManager txManager, StreamTestTableFactory tables) {
        return new StreamTest2StreamStore(txManager, tables);
    }

    /**
     * This should only be used by test code or as a performance optimization.
     */
    static StreamTest2StreamStore of(StreamTestTableFactory tables) {
        return new StreamTest2StreamStore(null, tables);
    }

    @Override
    protected long getInMemoryThreshold() {
        return IN_MEMORY_THRESHOLD;
    }

    @Override
    protected void storeBlock(Transaction t, Long id, long blockNumber, final byte[] block, final long duration, final TimeUnit unit) {
        Preconditions.checkArgument(block.length <= BLOCK_SIZE_IN_BYTES, "Block to store in DB must be less than BLOCK_SIZE_IN_BYTES");
        final StreamTest2StreamValueTable.StreamTest2StreamValueRow row = StreamTest2StreamValueTable.StreamTest2StreamValueRow.of(id, blockNumber);
        try {
            // Do a touch operation on this table to ensure we get a conflict if someone cleans it up.
            touchMetadataWhileStoringForConflicts(t, row.getId(), row.getBlockId(), duration, unit);
            tables.getStreamTest2StreamValueTable(t).putValue(row, block, duration, unit);
        } catch (RuntimeException e) {
            log.error("Error storing block " + row.getBlockId() + " for stream id " + row.getId(), e);
            throw e;
        }
    }

    private void touchMetadataWhileStoringForConflicts(Transaction t, Long id, long blockNumber, long duration, TimeUnit unit) {
        StreamTest2StreamMetadataTable metaTable = tables.getStreamTest2StreamMetadataTable(t);
        StreamTest2StreamMetadataTable.StreamTest2StreamMetadataRow row = StreamTest2StreamMetadataTable.StreamTest2StreamMetadataRow.of(id);
        StreamMetadata metadata = metaTable.getMetadatas(ImmutableSet.of(row)).values().iterator().next();
        Preconditions.checkState(metadata.getStatus() == Status.STORING, "This stream is being cleaned up while storing blocks: " + id);
        Builder builder = StreamMetadata.newBuilder(metadata);
        builder.setLength(blockNumber * BLOCK_SIZE_IN_BYTES + 1);
        metaTable.putMetadata(row, builder.build(), duration, unit);
    }

    @Override
    protected void putMetadataAndHashIndexTask(Transaction t, Map<Long, StreamMetadata> streamIdsToMetadata, long duration, TimeUnit unit) {
        StreamTest2StreamMetadataTable mdTable = tables.getStreamTest2StreamMetadataTable(t);
        Map<Long, StreamMetadata> prevMetadatas = getMetadata(t, streamIdsToMetadata.keySet());

        Map<StreamTest2StreamMetadataTable.StreamTest2StreamMetadataRow, StreamMetadata> rowsToStoredMetadata = Maps.newHashMap();
        Map<StreamTest2StreamMetadataTable.StreamTest2StreamMetadataRow, StreamMetadata> rowsToUnstoredMetadata = Maps.newHashMap();
        for (Entry<Long, StreamMetadata> e : streamIdsToMetadata.entrySet()) {
            Long streamId = e.getKey();
            StreamMetadata metadata = e.getValue();
            StreamMetadata prevMetadata = prevMetadatas.get(streamId);
            if (metadata.getStatus() == Status.STORED) {
                if (prevMetadata == null || prevMetadata.getStatus() != Status.STORING) {
                    // This can happen if we cleanup old streams.
                    throw new TransactionFailedRetriableException("Cannot mark a stream as stored that isn't currently storing: " + prevMetadata);
                }
                rowsToStoredMetadata.put(StreamTest2StreamMetadataTable.StreamTest2StreamMetadataRow.of(streamId), metadata);
            } else if (metadata.getStatus() == Status.STORING) {
                // This will prevent two users trying to store the same id.
                if (prevMetadata != null) {
                    throw new TransactionFailedRetriableException("Cannot reuse the same stream id: " + streamId);
                }
                rowsToUnstoredMetadata.put(StreamTest2StreamMetadataTable.StreamTest2StreamMetadataRow.of(streamId), metadata);
            }
        }
        putHashIndexTask(t, rowsToStoredMetadata, duration, unit);

        Map<StreamTest2StreamMetadataTable.StreamTest2StreamMetadataRow, StreamMetadata> rowsToMetadata = Maps.newHashMap();
        rowsToMetadata.putAll(rowsToStoredMetadata);
        rowsToMetadata.putAll(rowsToUnstoredMetadata);
        mdTable.putMetadata(rowsToMetadata, duration, unit);
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
        StreamTest2StreamValueTable.StreamTest2StreamValueRow row = StreamTest2StreamValueTable.StreamTest2StreamValueRow.of(streamId, blockId);
        try {
            os.write(getBlock(t, row));
        } catch (RuntimeException e) {
            log.error("Error getting block " + row.getBlockId() + " of stream " + row.getId(), e);
            throw e;
        } catch (IOException e) {
            log.error("Error writing block " + row.getBlockId() + " to file when getting stream id " + row.getId(), e);
            throw Throwables.rewrapAndThrowUncheckedException("Error writing blocks to file when creating stream.", e);
        }
    }

    private byte[] getBlock(Transaction t, StreamTest2StreamValueTable.StreamTest2StreamValueRow row) {
        StreamTest2StreamValueTable valueTable = tables.getStreamTest2StreamValueTable(t);
        return valueTable.getValues(ImmutableSet.of(row)).get(row);
    }

    @Override
    protected Map<Long, StreamMetadata> getMetadata(Transaction t, Set<Long> streamIds) {
        if (streamIds.isEmpty()) {
            return ImmutableMap.of();
        }
        StreamTest2StreamMetadataTable table = tables.getStreamTest2StreamMetadataTable(t);
        Map<StreamTest2StreamMetadataTable.StreamTest2StreamMetadataRow, StreamMetadata> metadatas = table.getMetadatas(getMetadataRowsForIds(streamIds));
        Map<Long, StreamMetadata> ret = Maps.newHashMap();
        for (Map.Entry<StreamTest2StreamMetadataTable.StreamTest2StreamMetadataRow, StreamMetadata> e : metadatas.entrySet()) {
            ret.put(e.getKey().getId(), e.getValue());
        }
        return ret;
    }

    @Override
    public Map<Sha256Hash, Long> lookupStreamIdsByHash(Transaction t, final Set<Sha256Hash> hashes) {
        if (hashes.isEmpty()) {
            return ImmutableMap.of();
        }
        StreamTest2StreamHashAidxTable idx = tables.getStreamTest2StreamHashAidxTable(t);
        Set<StreamTest2StreamHashAidxTable.StreamTest2StreamHashAidxRow> rows = getHashIndexRowsForHashes(hashes);

        Multimap<StreamTest2StreamHashAidxTable.StreamTest2StreamHashAidxRow, StreamTest2StreamHashAidxTable.StreamTest2StreamHashAidxColumnValue> m = idx.getRowsMultimap(rows);
        Map<Long, Sha256Hash> hashForStreams = Maps.newHashMap();
        for (StreamTest2StreamHashAidxTable.StreamTest2StreamHashAidxRow r : m.keySet()) {
            for (StreamTest2StreamHashAidxTable.StreamTest2StreamHashAidxColumnValue v : m.get(r)) {
                Long streamId = v.getColumnName().getStreamId();
                Sha256Hash hash = r.getHash();
                if (hashForStreams.containsKey(streamId)) {
                    AssertUtils.assertAndLog(hashForStreams.get(streamId).equals(hash), "(BUG) Stream ID has 2 different hashes: " + streamId);
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

    private Set<StreamTest2StreamHashAidxTable.StreamTest2StreamHashAidxRow> getHashIndexRowsForHashes(final Set<Sha256Hash> hashes) {
        Set<StreamTest2StreamHashAidxTable.StreamTest2StreamHashAidxRow> rows = Sets.newHashSet();
        for (Sha256Hash h : hashes) {
            rows.add(StreamTest2StreamHashAidxTable.StreamTest2StreamHashAidxRow.of(h));
        }
        return rows;
    }

    private Set<StreamTest2StreamMetadataTable.StreamTest2StreamMetadataRow> getMetadataRowsForIds(final Iterable<Long> ids) {
        Set<StreamTest2StreamMetadataTable.StreamTest2StreamMetadataRow> rows = Sets.newHashSet();
        for (Long id : ids) {
            rows.add(StreamTest2StreamMetadataTable.StreamTest2StreamMetadataRow.of(id));
        }
        return rows;
    }

    private void putHashIndexTask(Transaction t, Map<StreamTest2StreamMetadataTable.StreamTest2StreamMetadataRow, StreamMetadata> rowsToMetadata, long duration, TimeUnit unit) {
        Multimap<StreamTest2StreamHashAidxTable.StreamTest2StreamHashAidxRow, StreamTest2StreamHashAidxTable.StreamTest2StreamHashAidxColumnValue> indexMap = HashMultimap.create();
        for (Entry<StreamTest2StreamMetadataTable.StreamTest2StreamMetadataRow, StreamMetadata> e : rowsToMetadata.entrySet()) {
            StreamTest2StreamMetadataTable.StreamTest2StreamMetadataRow row = e.getKey();
            StreamMetadata metadata = e.getValue();
            Preconditions.checkArgument(
                    metadata.getStatus() == Status.STORED,
                    "Should only index successfully stored streams.");

            Sha256Hash hash = Sha256Hash.EMPTY;
            if (metadata.getHash() != com.google.protobuf.ByteString.EMPTY) {
                hash = new Sha256Hash(metadata.getHash().toByteArray());
            }
            StreamTest2StreamHashAidxTable.StreamTest2StreamHashAidxRow hashRow = StreamTest2StreamHashAidxTable.StreamTest2StreamHashAidxRow.of(hash);
            StreamTest2StreamHashAidxTable.StreamTest2StreamHashAidxColumn column = StreamTest2StreamHashAidxTable.StreamTest2StreamHashAidxColumn.of(row.getId());
            StreamTest2StreamHashAidxTable.StreamTest2StreamHashAidxColumnValue columnValue = StreamTest2StreamHashAidxTable.StreamTest2StreamHashAidxColumnValue.of(column, 0L);
            indexMap.put(hashRow, columnValue);
        }
        StreamTest2StreamHashAidxTable hiTable = tables.getStreamTest2StreamHashAidxTable(t);
        hiTable.put(indexMap, duration, unit);
    }

    /**
     * This should only be used from the cleanup tasks.
     */
    void deleteStreams(Transaction t, final Set<Long> streamIds) {
        if (streamIds.isEmpty()) {
            return;
        }
        Set<StreamTest2StreamMetadataTable.StreamTest2StreamMetadataRow> smRows = Sets.newHashSet();
        Multimap<StreamTest2StreamHashAidxTable.StreamTest2StreamHashAidxRow, StreamTest2StreamHashAidxTable.StreamTest2StreamHashAidxColumn> shToDelete = HashMultimap.create();
        for (Long streamId : streamIds) {
            smRows.add(StreamTest2StreamMetadataTable.StreamTest2StreamMetadataRow.of(streamId));
        }
        StreamTest2StreamMetadataTable table = tables.getStreamTest2StreamMetadataTable(t);
        Map<StreamTest2StreamMetadataTable.StreamTest2StreamMetadataRow, StreamMetadata> metadatas = table.getMetadatas(smRows);
        Set<StreamTest2StreamValueTable.StreamTest2StreamValueRow> streamValueToDelete = Sets.newHashSet();
        for (Entry<StreamTest2StreamMetadataTable.StreamTest2StreamMetadataRow, StreamMetadata> e : metadatas.entrySet()) {
            Long streamId = e.getKey().getId();
            long blocks = getNumberOfBlocksFromMetadata(e.getValue());
            for (long i = 0; i < blocks; i++) {
                streamValueToDelete.add(StreamTest2StreamValueTable.StreamTest2StreamValueRow.of(streamId, i));
            }
            ByteString streamHash = e.getValue().getHash();
            Sha256Hash hash = Sha256Hash.EMPTY;
            if (streamHash != com.google.protobuf.ByteString.EMPTY) {
                hash = new Sha256Hash(streamHash.toByteArray());
            } else {
                log.error("Empty hash for stream " + streamId);
            }
            StreamTest2StreamHashAidxTable.StreamTest2StreamHashAidxRow hashRow = StreamTest2StreamHashAidxTable.StreamTest2StreamHashAidxRow.of(hash);
            StreamTest2StreamHashAidxTable.StreamTest2StreamHashAidxColumn column = StreamTest2StreamHashAidxTable.StreamTest2StreamHashAidxColumn.of(streamId);
            shToDelete.put(hashRow, column);
        }
        tables.getStreamTest2StreamHashAidxTable(t).delete(shToDelete);
        tables.getStreamTest2StreamValueTable(t).delete(streamValueToDelete);
        table.delete(smRows);
    }

    static final int dummy = 0;
}
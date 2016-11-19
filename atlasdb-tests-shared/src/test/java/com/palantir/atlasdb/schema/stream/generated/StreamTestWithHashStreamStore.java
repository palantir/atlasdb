package com.palantir.atlasdb.schema.stream.generated;

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
import java.util.Set;
import java.util.concurrent.TimeUnit;

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
import com.google.common.io.CountingInputStream;
import com.google.common.primitives.Ints;
import com.google.protobuf.ByteString;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.protos.generated.StreamPersistence.Status;
import com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata;
import com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata.Builder;
import com.palantir.atlasdb.stream.AbstractPersistentStreamStore;
import com.palantir.atlasdb.stream.PersistentStreamStore;
import com.palantir.atlasdb.stream.StreamCleanedException;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.atlasdb.transaction.impl.TxTask;
import com.palantir.common.base.Throwables;
import com.palantir.common.compression.LZ4CompressingInputStream;
import com.palantir.common.compression.LZ4DecompressingInputStream;
import com.palantir.common.io.ConcatenatedInputStream;
import com.palantir.util.AssertUtils;
import com.palantir.util.ByteArrayIOStream;
import com.palantir.util.Pair;
import com.palantir.util.crypto.Sha256Hash;
import com.palantir.util.file.DeleteOnCloseFileInputStream;
import com.palantir.util.file.TempFileUtils;

@Generated("com.palantir.atlasdb.table.description.render.StreamStoreRenderer")
public final class StreamTestWithHashStreamStore extends AbstractPersistentStreamStore {
    public static final int BLOCK_SIZE_IN_BYTES = 1000000; // 1MB. DO NOT CHANGE THIS WITHOUT AN UPGRADE TASK
    public static final int IN_MEMORY_THRESHOLD = 4000; // streams under this size are kept in memory when loaded
    public static final String STREAM_FILE_PREFIX = "StreamTestWithHash_stream_";
    public static final String STREAM_FILE_SUFFIX = ".tmp";

    private static final Logger log = LoggerFactory.getLogger(StreamTestWithHashStreamStore.class);

    private final StreamTestTableFactory tables;

    private StreamTestWithHashStreamStore(TransactionManager txManager, StreamTestTableFactory tables) {
        super(txManager);
        this.tables = tables;
    }

    public static StreamTestWithHashStreamStore of(TransactionManager txManager, StreamTestTableFactory tables) {
        return new StreamTestWithHashStreamStore(txManager, tables);
    }

    /**
     * This should only be used by test code or as a performance optimization.
     */
    static StreamTestWithHashStreamStore of(StreamTestTableFactory tables) {
        return new StreamTestWithHashStreamStore(null, tables);
    }

    @Override
    protected long getInMemoryThreshold() {
        return IN_MEMORY_THRESHOLD;
    }

    @Override
    protected void storeBlock(Transaction t, long id, long blockNumber, final byte[] block) {
        Preconditions.checkArgument(block.length <= BLOCK_SIZE_IN_BYTES, "Block to store in DB must be less than BLOCK_SIZE_IN_BYTES");
        final StreamTestWithHashStreamValueTable.StreamTestWithHashStreamValueRow row = StreamTestWithHashStreamValueTable.StreamTestWithHashStreamValueRow.of(id, blockNumber);
        try {
            // Do a touch operation on this table to ensure we get a conflict if someone cleans it up.
            touchMetadataWhileStoringForConflicts(t, row.getId(), row.getBlockId());
            tables.getStreamTestWithHashStreamValueTable(t).putValue(row, block);
        } catch (RuntimeException e) {
            log.error("Error storing block " + row.getBlockId() + " for stream id " + row.getId(), e);
            throw e;
        }
    }

    private void touchMetadataWhileStoringForConflicts(Transaction t, Long id, long blockNumber) {
        StreamTestWithHashStreamMetadataTable metaTable = tables.getStreamTestWithHashStreamMetadataTable(t);
        StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow row = StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow.of(id);
        StreamMetadata metadata = metaTable.getMetadatas(ImmutableSet.of(row)).values().iterator().next();
        Preconditions.checkState(metadata.getStatus() == Status.STORING, "This stream is being cleaned up while storing blocks: " + id);
        Builder builder = StreamMetadata.newBuilder(metadata);
        builder.setLength(blockNumber * BLOCK_SIZE_IN_BYTES + 1);
        metaTable.putMetadata(row, builder.build());
    }

    @Override
    protected void putMetadataAndHashIndexTask(Transaction t, Map<Long, StreamMetadata> streamIdsToMetadata) {
        StreamTestWithHashStreamMetadataTable mdTable = tables.getStreamTestWithHashStreamMetadataTable(t);
        Map<Long, StreamMetadata> prevMetadatas = getMetadata(t, streamIdsToMetadata.keySet());

        Map<StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow, StreamMetadata> rowsToStoredMetadata = Maps.newHashMap();
        Map<StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow, StreamMetadata> rowsToUnstoredMetadata = Maps.newHashMap();
        for (Entry<Long, StreamMetadata> e : streamIdsToMetadata.entrySet()) {
            long streamId = e.getKey();
            StreamMetadata metadata = e.getValue();
            StreamMetadata prevMetadata = prevMetadatas.get(streamId);
            if (metadata.getStatus() == Status.STORED) {
                if (prevMetadata == null || prevMetadata.getStatus() != Status.STORING) {
                    // This can happen if we cleanup old streams.
                    throw new TransactionFailedRetriableException("Cannot mark a stream as stored that isn't currently storing: " + prevMetadata);
                }
                rowsToStoredMetadata.put(StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow.of(streamId), metadata);
            } else if (metadata.getStatus() == Status.STORING) {
                // This will prevent two users trying to store the same id.
                if (prevMetadata != null) {
                    throw new TransactionFailedRetriableException("Cannot reuse the same stream id: " + streamId);
                }
                rowsToUnstoredMetadata.put(StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow.of(streamId), metadata);
            }
        }
        putHashIndexTask(t, rowsToStoredMetadata);

        Map<StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow, StreamMetadata> rowsToMetadata = Maps.newHashMap();
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
        StreamTestWithHashStreamValueTable.StreamTestWithHashStreamValueRow row = StreamTestWithHashStreamValueTable.StreamTestWithHashStreamValueRow.of(streamId, blockId);
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

    private byte[] getBlock(Transaction t, StreamTestWithHashStreamValueTable.StreamTestWithHashStreamValueRow row) {
        StreamTestWithHashStreamValueTable valueTable = tables.getStreamTestWithHashStreamValueTable(t);
        return valueTable.getValues(ImmutableSet.of(row)).get(row);
    }

    @Override
    public Pair<Long, Sha256Hash> storeStream(InputStream stream) {
        long id = storeEmptyMetadata();
        MessageDigest digest = Sha256Hash.getMessageDigest();
        InputStream compressedStream;
        try {
            compressedStream = new LZ4CompressingInputStream(new DigestInputStream(stream, digest));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        StreamMetadata metadata = storeBlocksAndGetFinalMetadata(null, id, compressedStream, false);
        ByteString hashByteString = ByteString.copyFrom(digest.digest());
        StreamMetadata correctedMetadata = StreamMetadata.newBuilder(metadata)
                .setHash(hashByteString)
                .build();
        storeMetadataAndIndex(id, correctedMetadata);
        return Pair.create(id, new Sha256Hash(correctedMetadata.getHash().toByteArray()));
    }

    @Override
    public Map<Long, Sha256Hash> storeStreams(final Transaction t, final Map<Long, InputStream> streams) {
        if (streams.isEmpty()) {
            return ImmutableMap.of();
        }

        Map<Long, StreamMetadata> idsToEmptyMetadata = Maps.transformValues(streams, Functions.constant(getEmptyMetadata()));
        putMetadataAndHashIndexTask(t, idsToEmptyMetadata);

        Map<Long, StreamMetadata> idsToMetadata = Maps.transformEntries(streams, (id, stream) -> {
            MessageDigest digest = Sha256Hash.getMessageDigest();
            InputStream compressedStream;
            try {
                compressedStream = new LZ4CompressingInputStream(new DigestInputStream(stream, digest));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            StreamMetadata metadata = storeBlocksAndGetFinalMetadata(t, id, compressedStream, false);
            ByteString hashByteString = ByteString.copyFrom(digest.digest());
            return StreamMetadata.newBuilder(metadata)
                    .setHash(hashByteString)
                    .build();
        });
        putMetadataAndHashIndexTask(t, idsToMetadata);

        Map<Long, Sha256Hash> hashes = Maps.transformValues(idsToMetadata,
                metadata -> new Sha256Hash(metadata.getHash().toByteArray()));
        return hashes;
    }

    @Override
    public InputStream loadStream(Transaction t, final Long id) {
        try {
            return new LZ4DecompressingInputStream(super.loadStream(t, id));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<Long, InputStream> loadStreams(Transaction t, Set<Long> ids) {
        Map<Long, InputStream> compressedStreams = super.loadStreams(t, ids);
        return Maps.transformValues(compressedStreams, stream -> {
            try {
                return new LZ4DecompressingInputStream(stream);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public File loadStreamAsFile(Transaction transaction, Long id) {
        StreamMetadata metadata = getMetadata(transaction, id);
        checkStreamStored(id, metadata);
        return loadToNewTempFileDecompressed(transaction, id, metadata);
    }

    private File loadToNewTempFileDecompressed(Transaction transaction, Long id, StreamMetadata metadata) {
        try {
            File file = createTempFile(id);
            writeDecompressedStreamToFile(transaction, id, metadata, file);
            return file;
        } catch (IOException e) {
            log.error("Could not create temp file for stream id " + id, e);
            throw Throwables.rewrapAndThrowUncheckedException("Could not create file to create stream.", e);
        }
    }

    private void writeDecompressedStreamToFile(Transaction transaction, Long id, StreamMetadata metadata, File file) throws FileNotFoundException {
        FileOutputStream fos = new FileOutputStream(file);
        try {
            tryWriteDecompressedStreamToFile(transaction, id, metadata, fos);
        } catch (IOException e) {
            log.error("Could not finish streaming blocks to file for stream " + id, e);
            throw Throwables.rewrapAndThrowUncheckedException("Error writing blocks while opening a stream.", e);
        } finally {
            try {
                fos.close();
            } catch (IOException e) {
                // Do nothing
            }
        }
    }

    private void tryWriteDecompressedStreamToFile(Transaction transaction, Long id, StreamMetadata metadata, FileOutputStream fos) throws IOException {
        long numBlocks = getNumberOfBlocksFromMetadata(metadata);
        InputStream blockStream = new StreamBlockInputStream(numBlocks, id, transaction);
        InputStream decompressingStream = new LZ4DecompressingInputStream(blockStream);
        byte[] buffer = new byte[BLOCK_SIZE_IN_BYTES];
        int length;
        while ((length = decompressingStream.read(buffer)) > 0) {
            fos.write(buffer, 0, length);
        }
        decompressingStream.close();
        fos.close();
    }

    private class StreamBlockInputStream extends InputStream {
        private byte[] blockBytes;
        private long numBlocks;
        private int currentBlock;
        private int position;
        private final Long streamId;
        private final Transaction transaction;

        public StreamBlockInputStream(long numBlocks, Long streamId, Transaction transaction) {
            this.blockBytes = new byte[0];
            this.numBlocks = numBlocks;
            this.currentBlock = 0;
            this.position = 0;
            this.streamId = streamId;
            this.transaction = transaction;
        }

        @Override
        public int read() throws IOException {
            if ((position == blockBytes.length) && !refill()) {
                return -1;
            }
            return blockBytes[position++] & 0xff;
        }

        @Override
        public int read(byte b[], int off, int len) throws IOException {
            if (b == null) {
                throw new NullPointerException();
            } else if (off < 0 || len < 0 || len > b.length - off) {
                throw new IndexOutOfBoundsException();
            } else if (len == 0) {
                return 0;
            }

            int bytesRead = 0;
            while (bytesRead < len) {
                int remainingBuffer = blockBytes.length - position;
                int bytesToRead = Math.min(len - bytesRead, remainingBuffer);
                System.arraycopy(blockBytes, position, b, off + bytesRead, bytesToRead);
                position += bytesToRead;
                bytesRead += bytesToRead;
                if ((position == blockBytes.length) && !refill()) {
                    break;
                }
            }
            return bytesRead > 0 ? bytesRead : -1;
        }

        private boolean refill() {
            if (currentBlock == numBlocks) {
                return false;
            }
            blockBytes = getBlock(transaction, StreamTestWithHashStreamValueTable.StreamTestWithHashStreamValueRow.of(streamId, currentBlock));
            position = 0;
            currentBlock++;
            return true;
        }
    }

    @Override
    protected Map<Long, StreamMetadata> getMetadata(Transaction t, Set<Long> streamIds) {
        if (streamIds.isEmpty()) {
            return ImmutableMap.of();
        }
        StreamTestWithHashStreamMetadataTable table = tables.getStreamTestWithHashStreamMetadataTable(t);
        Map<StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow, StreamMetadata> metadatas = table.getMetadatas(getMetadataRowsForIds(streamIds));
        Map<Long, StreamMetadata> ret = Maps.newHashMap();
        for (Map.Entry<StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow, StreamMetadata> e : metadatas.entrySet()) {
            ret.put(e.getKey().getId(), e.getValue());
        }
        return ret;
    }

    @Override
    public Map<Sha256Hash, Long> lookupStreamIdsByHash(Transaction t, final Set<Sha256Hash> hashes) {
        if (hashes.isEmpty()) {
            return ImmutableMap.of();
        }
        StreamTestWithHashStreamHashAidxTable idx = tables.getStreamTestWithHashStreamHashAidxTable(t);
        Set<StreamTestWithHashStreamHashAidxTable.StreamTestWithHashStreamHashAidxRow> rows = getHashIndexRowsForHashes(hashes);

        Multimap<StreamTestWithHashStreamHashAidxTable.StreamTestWithHashStreamHashAidxRow, StreamTestWithHashStreamHashAidxTable.StreamTestWithHashStreamHashAidxColumnValue> m = idx.getRowsMultimap(rows);
        Map<Long, Sha256Hash> hashForStreams = Maps.newHashMap();
        for (StreamTestWithHashStreamHashAidxTable.StreamTestWithHashStreamHashAidxRow r : m.keySet()) {
            for (StreamTestWithHashStreamHashAidxTable.StreamTestWithHashStreamHashAidxColumnValue v : m.get(r)) {
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

    private Set<StreamTestWithHashStreamHashAidxTable.StreamTestWithHashStreamHashAidxRow> getHashIndexRowsForHashes(final Set<Sha256Hash> hashes) {
        Set<StreamTestWithHashStreamHashAidxTable.StreamTestWithHashStreamHashAidxRow> rows = Sets.newHashSet();
        for (Sha256Hash h : hashes) {
            rows.add(StreamTestWithHashStreamHashAidxTable.StreamTestWithHashStreamHashAidxRow.of(h));
        }
        return rows;
    }

    private Set<StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow> getMetadataRowsForIds(final Iterable<Long> ids) {
        Set<StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow> rows = Sets.newHashSet();
        for (Long id : ids) {
            rows.add(StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow.of(id));
        }
        return rows;
    }

    private void putHashIndexTask(Transaction t, Map<StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow, StreamMetadata> rowsToMetadata) {
        Multimap<StreamTestWithHashStreamHashAidxTable.StreamTestWithHashStreamHashAidxRow, StreamTestWithHashStreamHashAidxTable.StreamTestWithHashStreamHashAidxColumnValue> indexMap = HashMultimap.create();
        for (Entry<StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow, StreamMetadata> e : rowsToMetadata.entrySet()) {
            StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow row = e.getKey();
            StreamMetadata metadata = e.getValue();
            Preconditions.checkArgument(
                    metadata.getStatus() == Status.STORED,
                    "Should only index successfully stored streams.");

            Sha256Hash hash = Sha256Hash.EMPTY;
            if (metadata.getHash() != com.google.protobuf.ByteString.EMPTY) {
                hash = new Sha256Hash(metadata.getHash().toByteArray());
            }
            StreamTestWithHashStreamHashAidxTable.StreamTestWithHashStreamHashAidxRow hashRow = StreamTestWithHashStreamHashAidxTable.StreamTestWithHashStreamHashAidxRow.of(hash);
            StreamTestWithHashStreamHashAidxTable.StreamTestWithHashStreamHashAidxColumn column = StreamTestWithHashStreamHashAidxTable.StreamTestWithHashStreamHashAidxColumn.of(row.getId());
            StreamTestWithHashStreamHashAidxTable.StreamTestWithHashStreamHashAidxColumnValue columnValue = StreamTestWithHashStreamHashAidxTable.StreamTestWithHashStreamHashAidxColumnValue.of(column, 0L);
            indexMap.put(hashRow, columnValue);
        }
        StreamTestWithHashStreamHashAidxTable hiTable = tables.getStreamTestWithHashStreamHashAidxTable(t);
        hiTable.put(indexMap);
    }

    /**
     * This should only be used from the cleanup tasks.
     */
    void deleteStreams(Transaction t, final Set<Long> streamIds) {
        if (streamIds.isEmpty()) {
            return;
        }
        Set<StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow> smRows = Sets.newHashSet();
        Multimap<StreamTestWithHashStreamHashAidxTable.StreamTestWithHashStreamHashAidxRow, StreamTestWithHashStreamHashAidxTable.StreamTestWithHashStreamHashAidxColumn> shToDelete = HashMultimap.create();
        for (Long streamId : streamIds) {
            smRows.add(StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow.of(streamId));
        }
        StreamTestWithHashStreamMetadataTable table = tables.getStreamTestWithHashStreamMetadataTable(t);
        Map<StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow, StreamMetadata> metadatas = table.getMetadatas(smRows);
        Set<StreamTestWithHashStreamValueTable.StreamTestWithHashStreamValueRow> streamValueToDelete = Sets.newHashSet();
        for (Entry<StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow, StreamMetadata> e : metadatas.entrySet()) {
            Long streamId = e.getKey().getId();
            long blocks = getNumberOfBlocksFromMetadata(e.getValue());
            for (long i = 0; i < blocks; i++) {
                streamValueToDelete.add(StreamTestWithHashStreamValueTable.StreamTestWithHashStreamValueRow.of(streamId, i));
            }
            ByteString streamHash = e.getValue().getHash();
            Sha256Hash hash = Sha256Hash.EMPTY;
            if (streamHash != com.google.protobuf.ByteString.EMPTY) {
                hash = new Sha256Hash(streamHash.toByteArray());
            } else {
                log.error("Empty hash for stream " + streamId);
            }
            StreamTestWithHashStreamHashAidxTable.StreamTestWithHashStreamHashAidxRow hashRow = StreamTestWithHashStreamHashAidxTable.StreamTestWithHashStreamHashAidxRow.of(hash);
            StreamTestWithHashStreamHashAidxTable.StreamTestWithHashStreamHashAidxColumn column = StreamTestWithHashStreamHashAidxTable.StreamTestWithHashStreamHashAidxColumn.of(streamId);
            shToDelete.put(hashRow, column);
        }
        tables.getStreamTestWithHashStreamHashAidxTable(t).delete(shToDelete);
        tables.getStreamTestWithHashStreamValueTable(t).delete(streamValueToDelete);
        table.delete(smRows);
    }

    @Override
    protected void markStreamsAsUsedInternal(Transaction t, final Map<Long, byte[]> streamIdsToReference) {
        if (streamIdsToReference.isEmpty()) {
            return;
        }
        StreamTestWithHashStreamIdxTable index = tables.getStreamTestWithHashStreamIdxTable(t);
        Multimap<StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxRow, StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxColumnValue> rowsToValues = HashMultimap.create();
        for (Map.Entry<Long, byte[]> entry : streamIdsToReference.entrySet()) {
            Long streamId = entry.getKey();
            byte[] reference = entry.getValue();
            StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxColumn col = StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxColumn.of(reference);
            StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxColumnValue value = StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxColumnValue.of(col, 0L);
            rowsToValues.put(StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxRow.of(streamId), value);
        }
        index.put(rowsToValues);
    }

    @Override
    public void unmarkStreamsAsUsed(Transaction t, final Map<Long, byte[]> streamIdsToReference) {
        if (streamIdsToReference.isEmpty()) {
            return;
        }
        StreamTestWithHashStreamIdxTable index = tables.getStreamTestWithHashStreamIdxTable(t);
        Multimap<StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxRow, StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxColumn> toDelete = ArrayListMultimap.create(streamIdsToReference.size(), 1);
        for (Map.Entry<Long, byte[]> entry : streamIdsToReference.entrySet()) {
            Long streamId = entry.getKey();
            byte[] reference = entry.getValue();
            StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxColumn col = StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxColumn.of(reference);
            toDelete.put(StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxRow.of(streamId), col);
        }
        index.delete(toDelete);
    }

    @Override
    protected void touchMetadataWhileMarkingUsedForConflicts(Transaction t, Iterable<Long> ids) {
        StreamTestWithHashStreamMetadataTable metaTable = tables.getStreamTestWithHashStreamMetadataTable(t);
        Set<StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow> rows = Sets.newHashSet();
        for (Long id : ids) {
            rows.add(StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow.of(id));
        }
        Map<StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow, StreamMetadata> metadatas = metaTable.getMetadatas(rows);
        for (Map.Entry<StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow, StreamMetadata> e : metadatas.entrySet()) {
            StreamMetadata metadata = e.getValue();
            Preconditions.checkState(metadata.getStatus() == Status.STORED,
            "Stream: " + e.getKey().getId() + " has status: " + metadata.getStatus());
            metaTable.putMetadata(e.getKey(), metadata);
        }
        SetView<StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow> missingRows = Sets.difference(rows, metadatas.keySet());
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
     * {@link BufferedInputStream}
     * {@link Builder}
     * {@link ByteArrayIOStream}
     * {@link ByteArrayInputStream}
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
     * {@link LZ4CompressingInputStream}
     * {@link LZ4DecompressingInputStream}
     * {@link List}
     * {@link Lists}
     * {@link Logger}
     * {@link LoggerFactory}
     * {@link Map}
     * {@link Maps}
     * {@link MessageDigest}
     * {@link Multimap}
     * {@link Multimaps}
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
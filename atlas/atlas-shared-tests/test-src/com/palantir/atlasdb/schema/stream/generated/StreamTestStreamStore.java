// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import com.palantir.atlasdb.stream.ExpiringStreamStore;
import com.palantir.atlasdb.stream.PersistentStreamStore;
import com.palantir.atlasdb.stream.TransactionBackedRunner;
import com.palantir.atlasdb.stream.TransactionManagerBackedRunner;
import com.palantir.atlasdb.stream.TransactionRunner;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.atlasdb.transaction.impl.TxTask;
import com.palantir.common.base.Throwables;
import com.palantir.common.io.ConcatenatedInputStream;
import com.palantir.util.AssertUtils;
import com.palantir.util.ByteArrayIOStream;
import com.palantir.util.crypto.Sha256Hash;
import com.palantir.util.file.DeleteOnCloseFileInputStream;
import com.palantir.util.file.FileUtils;


public class StreamTestStreamStore implements PersistentStreamStore<Long> {
    public static final int BLOCK_SIZE_IN_BYTES = 1000000; // 1MB. DO NOT CHANGE THIS WITHOUT AN UPGRADE TASK
    public static final int IN_MEMORY_THRESHOLD = 4000; // streams under this size are kept in memory when loaded
    public static final String STREAM_FILE_PREFIX = "StreamTest_stream_";
    public static final String STREAM_FILE_SUFFIX = ".tmp";

    private static final Logger log = LoggerFactory.getLogger(StreamTestStreamStore.class);

    private final TransactionRunner txRunner;
    private final StreamTestTableFactory tables;

    private StreamTestStreamStore(TransactionManager txManager, StreamTestTableFactory tables) {
        this.txRunner = new TransactionManagerBackedRunner(txManager);
        this.tables = tables;
    }

    private StreamTestStreamStore(Transaction t, StreamTestTableFactory tables) {
        this.txRunner = new TransactionBackedRunner(t);
        this.tables = tables;
    }

    public static StreamTestStreamStore of(TransactionManager txManager, StreamTestTableFactory tables) {
        return new StreamTestStreamStore(txManager, tables);
    }

    /**
     * This should only be used by test code or as a performance optimization.
     */
    public static StreamTestStreamStore of(final Transaction t, StreamTestTableFactory tables) {
        return new StreamTestStreamStore(t, tables);
    }

    // =========================================================================
    // General Storing Helpers
    // =========================================================================

    /**
     * @return metadata, the final metadata that should be stored into the database if the stream hash is correct and unique
     */
    private StreamMetadata storeBlocksAndGetFinalMetadata(Long id, InputStream stream) {
        // Set up for finding hash and length
        MessageDigest digest = Sha256Hash.getMessageDigest();
        stream = new DigestInputStream(stream, digest);
        CountingInputStream countingStream = new CountingInputStream(stream);

        // Try to store the bytes to the stream and get length
        try {
            storeBlocksFromStream(id, countingStream);
        } catch (IOException e) {
            long length = countingStream.getCount();
            StreamMetadata metadata = StreamMetadata.newBuilder()
                .setStatus(Status.FAILED)
                .setLength(length)
                .setHash(com.google.protobuf.ByteString.EMPTY)
                .build();
            storeMetadataAndIndex(id, metadata);
            log.error("Could not store stream " + id + ". Failed after " + length + " bytes.", e);
            throw Throwables.rewrapAndThrowUncheckedException("Failed to store stream.", e);
        }

        // Get hash and length
        ByteString hashByteString = ByteString.copyFrom(digest.digest());
        long length = countingStream.getCount();

        // Return the final metadata.
        StreamMetadata metadata = StreamMetadata.newBuilder()
            .setStatus(Status.STORED)
            .setLength(length)
            .setHash(hashByteString)
            .build();
        return metadata;
    }

    // General Storing Helpers: Storing blocks
    // =========================================================================

    private void storeBlocksFromStream(Long id, InputStream stream) throws IOException {
        // We need to use a buffered stream here because we assume each read will fill the whole buffer.
        stream = new BufferedInputStream(stream);
        byte[] bytesToStore = new byte[BLOCK_SIZE_IN_BYTES];
        long blockNumber = 0;

        // Read initial block.
        int length = stream.read(bytesToStore);

        // Stops when there is nothing left to read.
        while (length != -1) {
            // Store only relevant data if it only filled a partial block
            if (length < BLOCK_SIZE_IN_BYTES) {
                bytesToStore = Arrays.copyOf(bytesToStore, length);
            }

            // Store block to database.
            storeBlock(StreamTestStreamValueTable.StreamTestStreamValueRow.of(id, blockNumber), bytesToStore);

            // Get a new block.
            blockNumber++;
            bytesToStore = new byte[BLOCK_SIZE_IN_BYTES];
            length = stream.read(bytesToStore);
        }
    }

    private void storeBlock(final StreamTestStreamValueTable.StreamTestStreamValueRow row, final byte[] block) {
        Preconditions.checkArgument(block.length <= BLOCK_SIZE_IN_BYTES, "Block to store in DB must be less than BLOCK_SIZE_IN_BYTES");

        try {
            txRunner.run(new TransactionTask<Void, RuntimeException>() {
                @Override
                public Void execute(Transaction t) {
                    // Do a touch operation on this table to ensure we get a conflict if someone cleans it up.
                    touchMetadataWhileStoringForConflicts(t, row.getId(), row.getBlockId());
                    tables.getStreamTestStreamValueTable(t).putValue(row, block);
                    return null;
                }
            });
        } catch (RuntimeException e) {
            log.error("Error storing block " + row.getBlockId() + " for stream id " + row.getId(), e);
            throw e;
        }
    }

    private void touchMetadataWhileStoringForConflicts(Transaction t, Long id, long blockNumber) {
        StreamTestStreamMetadataTable metaTable = tables.getStreamTestStreamMetadataTable(t);
        StreamTestStreamMetadataTable.StreamTestStreamMetadataRow row = StreamTestStreamMetadataTable.StreamTestStreamMetadataRow.of(id);
        StreamMetadata metadata = metaTable.getMetadatas(ImmutableSet.of(row)).values().iterator().next();
        Preconditions.checkState(metadata.getStatus() == Status.STORING, "This stream is being cleaned up while storing blocks: " + id);
        Builder builder = StreamMetadata.newBuilder(metadata);
        builder.setLength(blockNumber * BLOCK_SIZE_IN_BYTES + 1);
        metaTable.putMetadata(row, builder.build());
    }

    // General Storing Helpers: Storing Metadata
    // =========================================================================

    private void storeMetadataAndIndex(final Long streamId, final StreamMetadata metadata) {
        txRunner.run(new TransactionTask<Void, RuntimeException>() {
            @Override
            public Void execute(Transaction t) {
                StreamTestStreamMetadataTable.StreamTestStreamMetadataRow row = StreamTestStreamMetadataTable.StreamTestStreamMetadataRow.of(streamId);
                putMetadataAndHashIndexTask(t, row, metadata);
                return null;
            }
        });
    }

    private void putMetadataAndHashIndexTask(Transaction t, StreamTestStreamMetadataTable.StreamTestStreamMetadataRow row, StreamMetadata metadata) {
        StreamTestStreamMetadataTable mdTable = tables.getStreamTestStreamMetadataTable(t);
        if (metadata.getStatus() == Status.STORED) {
            StreamMetadata prevMetadata = getMetadataTask(t, row);
            if (prevMetadata == null || prevMetadata.getStatus() != Status.STORING) {
                // This can happen if we cleanup old streams.
                throw new IllegalStateException("Cannot mark a stream as stored that isn't currently storing: " + prevMetadata);
            }
            putHashIndexTask(t, row, metadata);
        } else if (metadata.getStatus() == Status.STORING) {
            StreamMetadata prevMetadata = getMetadataTask(t, row);
            // This will prevent two users trying to store the same id.
            if (prevMetadata != null) {
                throw new IllegalStateException("Cannot reuse the same stream id: " + row.getId());
            }
        }

        mdTable.putMetadata(row, metadata);
    }

    // General Storing Helpers
    // =========================================================================

    private long getNumberOfBlocksFromMetadata(StreamMetadata metadata) {
        return (metadata.getLength() + BLOCK_SIZE_IN_BYTES - 1) / BLOCK_SIZE_IN_BYTES;
    }

    // =========================================================================
    // Store Streams
    // =========================================================================

    @Override
    public Sha256Hash storeStream(Long id, InputStream stream) {
        // Store empty metadata before doing anything --- this is will be useful
        // once we implement stream garbage collection.
        storeMetadataAndIndex(id, getEmptyMetadata());

        StreamMetadata metadata = storeBlocksAndGetFinalMetadata(id, stream);
        storeMetadataAndIndex(id, metadata);
        return new Sha256Hash(metadata.getHash().toByteArray());
    }

    @Override
    public Map<Long, Sha256Hash> storeStreams(final Map<Long, InputStream> streams) {
        if (streams.isEmpty()) {
            return ImmutableMap.of();
        }
        txRunner.run(new TransactionTask<Void, RuntimeException>() {
            @Override
            public Void execute(Transaction t) {
                StreamTestStreamMetadataTable table = tables.getStreamTestStreamMetadataTable(t);
                Set<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow> rows = getMetadataRowsForIds(streams.keySet());
                Map<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow, StreamMetadata> existingMetadata = table.getMetadatas(rows);
                if (!existingMetadata.isEmpty()) {
                    throw new IllegalStateException("Cannot reuse existing stream ids: " + existingMetadata.keySet());
                }
                Map<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow, StreamTestStreamMetadataTable.Metadata> metadata = Maps.newHashMap();
                StreamMetadata emptyMetadata = getEmptyMetadata();
                for (StreamTestStreamMetadataTable.StreamTestStreamMetadataRow row : rows) {
                    metadata.put(row, StreamTestStreamMetadataTable.Metadata.of(emptyMetadata));
                }
                table.put(Multimaps.forMap(metadata));
                return null;
            }
        });
        Map<Long, Sha256Hash> hashes = Maps.newHashMap();
        for (Entry<Long, InputStream> entry : streams.entrySet()) {
            Long id = entry.getKey();
            InputStream stream = entry.getValue();
            StreamMetadata metadata = storeBlocksAndGetFinalMetadata(id, stream);
            storeMetadataAndIndex(id, metadata);
            hashes.put(id, new Sha256Hash(metadata.getHash().toByteArray()));
        }
        return hashes;
    }

    private StreamMetadata getEmptyMetadata() {
        return StreamMetadata.newBuilder()
            .setStatus(Status.STORING)
            .setLength(0L)
            .setHash(com.google.protobuf.ByteString.EMPTY)
            .build();
    }

    // =========================================================================
    // Load Streams
    // =========================================================================

    @Override
    public Map<Long, InputStream> loadStreams(final Set<Long> ids) {
        if (ids.isEmpty()) {
            return ImmutableMap.of();
        }
        Map<Long, StreamMetadata> metadata = txRunner.run(
                new TransactionTask<Map<Long, StreamMetadata>, RuntimeException>() {
            @Override
            public Map<Long, StreamMetadata> execute(Transaction t) {
                return getMetadataTask(t, ids);
            }
        });
        final Collection<StreamTestStreamValueTable.StreamTestStreamValueRow> rows = Lists.newArrayListWithExpectedSize(metadata.size());
        for (Entry<Long, StreamMetadata> entry : metadata.entrySet()) {
            Long id = entry.getKey();
            StreamMetadata m = checkStreamStored(id, entry.getValue());
            long numBlocks = getNumberOfBlocksFromMetadata(m);
            for (int blockId = 0; blockId < numBlocks; blockId++) {
                rows.add(StreamTestStreamValueTable.StreamTestStreamValueRow.of(id, blockId));
            }
        }
        Map<StreamTestStreamValueTable.StreamTestStreamValueRow, byte[]> values = txRunner.run(
                new TransactionTask<Map<StreamTestStreamValueTable.StreamTestStreamValueRow, byte[]>, RuntimeException>() {
            @Override
            public Map<StreamTestStreamValueTable.StreamTestStreamValueRow, byte[]> execute(Transaction t) {
                StreamTestStreamValueTable valueTable = tables.getStreamTestStreamValueTable(t);
                return valueTable.getValues(rows);
            }
        });
        Map<Long, InputStream> streams = Maps.newHashMapWithExpectedSize(ids.size());
        for (Entry<Long, StreamMetadata> entry : metadata.entrySet()) {
            Long id = entry.getKey();
            int numBlocks = (int) getNumberOfBlocksFromMetadata(entry.getValue());
            Collection<InputStream> fragments = Lists.newArrayListWithCapacity(numBlocks);
            for (int blockId = 0; blockId < numBlocks; blockId++) {
                fragments.add(new ByteArrayInputStream(values.get(StreamTestStreamValueTable.StreamTestStreamValueRow.of(id, blockId))));
            }
            streams.put(id, new ConcatenatedInputStream(fragments));
        }
        return streams;
    }

    @Override
    public InputStream loadStream(final Long id) {
        try {
            StreamMetadata metadata = checkStreamStored(id, getMetadata(StreamTestStreamMetadataTable.StreamTestStreamMetadataRow.of(id)));
            if (metadata.getLength() == 0) {
                return new ByteArrayInputStream(new byte[0]);
            } else if (metadata.getLength() <= Math.min(IN_MEMORY_THRESHOLD, BLOCK_SIZE_IN_BYTES)) {
                ByteArrayIOStream ios = new ByteArrayIOStream(Ints.saturatedCast(metadata.getLength()));
                writeSingleBlockToOutputStream(StreamTestStreamValueTable.StreamTestStreamValueRow.of(id, 0), ios);
                return ios.getInputStream();
            } else {
                File file = loadToNewTempFile(id, metadata);
                return new DeleteOnCloseFileInputStream(file);
            }
        } catch (FileNotFoundException e) {
            log.error("Error opening temp file for stream " + id, e);
            throw Throwables.rewrapAndThrowUncheckedException("Could not open temp file to create stream.", e);
        }
    }

    @Override
    public File loadStreamAsFile(Long id) {
        StreamMetadata metadata = checkStreamStored(id, getMetadata(StreamTestStreamMetadataTable.StreamTestStreamMetadataRow.of(id)));
        return loadToNewTempFile(id, metadata);
    }

    private File loadToNewTempFile(Long id, StreamMetadata metadata) {
        try {
            File file = createTempFile(id);
            writeStreamToFile(id, metadata, file);
            return file;
        } catch (IOException e) {
            log.error("Could not create temp file for stream id " + id, e);
            throw Throwables.rewrapAndThrowUncheckedException("Could not create file to create stream.", e);
        }
    }

    // Load Streams: checking metadata
    // =========================================================================

    private StreamMetadata checkStreamStored(Long id, StreamMetadata metadata) {
        if (metadata == null) {
            log.error("Error loading stream " + id + " because it was never stored.");
            throw new IllegalArgumentException("Unable to load stream " + id + " because it was never stored.");
        } else if (metadata.getStatus() != Status.STORED) {
            log.error("Error loading stream " + id + " because it has status " + metadata.getStatus());
            throw new IllegalArgumentException("Could not get stream because it was not fully stored.");
        }
        return metadata;
    }

    // Load Streams: File writing methods
    // =========================================================================

    private File createTempFile(Long id) throws IOException {
        File file = FileUtils.createTempFile(STREAM_FILE_PREFIX + id, STREAM_FILE_SUFFIX);
        file.deleteOnExit();
        return file;
    }

    private void writeStreamToFile(Long id, StreamMetadata metadata, File file) throws FileNotFoundException {
        FileOutputStream fos = new FileOutputStream(file);
        try {
            tryWriteStreamToFile(id, metadata, fos);
        } catch (IOException e) {
            log.error("Could not finish streaming blocks to file for stream " + id, e);
            throw Throwables.rewrapAndThrowUncheckedException("Error writing blocks while opening a stream.", e);
        } finally {
            IOUtils.closeQuietly(fos);
        }
    }

    private void tryWriteStreamToFile(Long id, StreamMetadata metadata, FileOutputStream fos) throws IOException {
        long numBlocks = getNumberOfBlocksFromMetadata(metadata);
        for (long i = 0; i < numBlocks; i++) {
            writeSingleBlockToOutputStream(StreamTestStreamValueTable.StreamTestStreamValueRow.of(id, i), fos);
        }
        fos.close();
    }

    private void writeSingleBlockToOutputStream(final StreamTestStreamValueTable.StreamTestStreamValueRow row, OutputStream os) {
        try {
            // Get block
            byte[] block = txRunner.run(new TransactionTask<byte[], RuntimeException>() {
                @Override
                public byte[] execute(Transaction t) {
                    return getBlockTask(t, row);
                }
            });

            os.write(block);
        } catch (RuntimeException e) {
            log.error("Error getting block " + row.getBlockId() + " of stream " + row.getId(), e);
            throw e;
        } catch (IOException e) {
            log.error("Error writing block " + row.getBlockId() + " to file when getting stream id " + row.getId(), e);
            throw Throwables.rewrapAndThrowUncheckedException("Error writing blocks to file when creating stream.", e);
        }
    }

    // Load Streams: Getting blocks
    // =========================================================================

    private byte[] getBlockTask(Transaction t, StreamTestStreamValueTable.StreamTestStreamValueRow row) {
        StreamTestStreamValueTable valueTable = tables.getStreamTestStreamValueTable(t);
        byte[] block = valueTable.getValues(ImmutableSet.of(row)).get(row);
        return block;
    }

    // Load Streams: Getting metadata
    // =========================================================================

    private StreamMetadata getMetadata(final StreamTestStreamMetadataTable.StreamTestStreamMetadataRow row) {
        try {
            return txRunner.run(new TransactionTask<StreamMetadata, RuntimeException>() {
                @Override
                public StreamMetadata execute(Transaction t) {
                    return getMetadataTask(t, row);
                }
            });
        } catch (RuntimeException e) {
            log.error("Error getting metadata for stream id " + row.getId(), e);
            throw e;
        }
    }

    private StreamMetadata getMetadataTask(Transaction t, StreamTestStreamMetadataTable.StreamTestStreamMetadataRow row) {
        StreamTestStreamMetadataTable table = tables.getStreamTestStreamMetadataTable(t);
        StreamMetadata metadata = table.getMetadatas(ImmutableSet.of(row)).get(row);
        return metadata;
    }

    private Map<Long, StreamMetadata> getMetadataTask(Transaction t, Set<Long> streamIds) {
        if (streamIds.isEmpty()) {
            return ImmutableMap.of();
        }
        StreamTestStreamMetadataTable table = tables.getStreamTestStreamMetadataTable(t);
        Map<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow, StreamMetadata> metadatas = table.getMetadatas(getMetadataRowsForIds(streamIds));
        Map<Long, StreamMetadata> ret = Maps.newHashMap();
        for (Map.Entry<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow, StreamMetadata> e : metadatas.entrySet()) {
            ret.put(e.getKey().getId(), e.getValue());
        }
        return ret;
    }

    // =========================================================================
    // Stuff related to hash indexing
    // =========================================================================

    @Override
    public Map<Sha256Hash, Long> lookupStreamIdsByHash(final Set<Sha256Hash> hashes) {
        if (hashes.isEmpty()) {
            return ImmutableMap.of();
        }
        return txRunner.run(new TransactionTask<Map<Sha256Hash, Long>, RuntimeException>() {
            @Override
            public Map<Sha256Hash, Long> execute(Transaction t) {
                StreamTestStreamHashAidxTable idx = tables.getStreamTestStreamHashAidxTable(t);
                Set<StreamTestStreamHashAidxTable.StreamTestStreamHashAidxRow> rows = getHashIndexRowsForHashes(hashes);

                Multimap<StreamTestStreamHashAidxTable.StreamTestStreamHashAidxRow, StreamTestStreamHashAidxTable.StreamTestStreamHashAidxColumnValue> m = idx.getRowsMultimap(rows);
                Map<Long, Sha256Hash> hashForStreams = Maps.newHashMap();
                for (StreamTestStreamHashAidxTable.StreamTestStreamHashAidxRow r : m.keySet()) {
                    for (StreamTestStreamHashAidxTable.StreamTestStreamHashAidxColumnValue v : m.get(r)) {
                        Long streamId = v.getColumnName().getStreamId();
                        Sha256Hash hash = r.getHash();
                        if (hashForStreams.containsKey(streamId)) {
                            AssertUtils.assertAndLog(hashForStreams.get(streamId).equals(hash), "(BUG) Stream ID has 2 different hashes: " + streamId);
                        }
                        hashForStreams.put(streamId, hash);
                    }
                }
                Map<Long, StreamMetadata> metadata = getMetadataTask(t, hashForStreams.keySet());

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
        });
    }

    private Set<StreamTestStreamHashAidxTable.StreamTestStreamHashAidxRow> getHashIndexRowsForHashes(final Set<Sha256Hash> hashes) {
        Set<StreamTestStreamHashAidxTable.StreamTestStreamHashAidxRow> rows = Sets.newHashSet();
        for (Sha256Hash h : hashes) {
            rows.add(StreamTestStreamHashAidxTable.StreamTestStreamHashAidxRow.of(h));
        }
        return rows;
    }

    private Set<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow> getMetadataRowsForIds(final Iterable<Long> ids) {
        Set<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow> rows = Sets.newHashSet();
        for (Long id : ids) {
            rows.add(StreamTestStreamMetadataTable.StreamTestStreamMetadataRow.of(id));
        }
        return rows;
    }

    private void putHashIndexTask(Transaction t, StreamTestStreamMetadataTable.StreamTestStreamMetadataRow row, StreamMetadata metadata) {
        Preconditions.checkArgument(
                metadata.getStatus() == Status.STORED,
                "Should only index successfully stored streams.");

        Sha256Hash hash = Sha256Hash.EMPTY;
        if (metadata.getHash() != com.google.protobuf.ByteString.EMPTY) {
            hash = new Sha256Hash(metadata.getHash().toByteArray());
        }
        StreamTestStreamHashAidxTable.StreamTestStreamHashAidxRow hashRow = StreamTestStreamHashAidxTable.StreamTestStreamHashAidxRow.of(hash);
        StreamTestStreamHashAidxTable.StreamTestStreamHashAidxColumn column = StreamTestStreamHashAidxTable.StreamTestStreamHashAidxColumn.of(row.getId());
        StreamTestStreamHashAidxTable.StreamTestStreamHashAidxColumnValue columnValue = StreamTestStreamHashAidxTable.StreamTestStreamHashAidxColumnValue.of(column, 0L);
        StreamTestStreamHashAidxTable hiTable = tables.getStreamTestStreamHashAidxTable(t);
        hiTable.put(hashRow, columnValue);
    }

    public void deleteStreams(final Set<Long> streamIds) {
        if (streamIds.isEmpty()) {
            return;
        }
        txRunner.run(new TransactionTask<Void, RuntimeException>() {
            @Override
            public Void execute(Transaction t) {
                Set<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow> smRows = Sets.newHashSet();
                Multimap<StreamTestStreamHashAidxTable.StreamTestStreamHashAidxRow, StreamTestStreamHashAidxTable.StreamTestStreamHashAidxColumn> shToDelete = HashMultimap.create();
                for (Long streamId : streamIds) {
                    smRows.add(StreamTestStreamMetadataTable.StreamTestStreamMetadataRow.of(streamId));
                }
                StreamTestStreamMetadataTable StreamTestStreamMetadataTable = tables.getStreamTestStreamMetadataTable(t);
                Map<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow, StreamMetadata> metadatas = StreamTestStreamMetadataTable.getMetadatas(smRows);

                Set<StreamTestStreamValueTable.StreamTestStreamValueRow> streamValueToDelete = Sets.newHashSet();
                for (Entry<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow, StreamMetadata> e : metadatas.entrySet()) {
                    Long streamId = e.getKey().getId();
                    long blocks = getNumberOfBlocksFromMetadata(e.getValue());
                    for (long i = 0; i < blocks; i++) {
                        streamValueToDelete.add(StreamTestStreamValueTable.StreamTestStreamValueRow.of(streamId, i));
                    }

                    ByteString streamHash = e.getValue().getHash();
                    Sha256Hash hash = Sha256Hash.EMPTY;
                    if (streamHash != com.google.protobuf.ByteString.EMPTY) {
                        hash = new Sha256Hash(streamHash.toByteArray());
                    } else {
                        log.error("Empty hash for stream " + streamId);
                    }
                    StreamTestStreamHashAidxTable.StreamTestStreamHashAidxRow hashRow = StreamTestStreamHashAidxTable.StreamTestStreamHashAidxRow.of(hash);
                    StreamTestStreamHashAidxTable.StreamTestStreamHashAidxColumn column = StreamTestStreamHashAidxTable.StreamTestStreamHashAidxColumn.of(streamId);
                    shToDelete.put(hashRow, column);
                }

                tables.getStreamTestStreamHashAidxTable(t).delete(shToDelete);
                tables.getStreamTestStreamValueTable(t).delete(streamValueToDelete);
                StreamTestStreamMetadataTable.delete(smRows);
                return null;
            }
        });
    }

    @Override
    public void markStreamsAsUsed(final Map<Long, byte[]> streamIdsToReference) {
        if (streamIdsToReference.isEmpty()) {
            return;
        }
        txRunner.run(new TxTask() {
            @Override
            public Void execute(Transaction t) {
                touchMetadataWhileMarkingUsedForConflicts(t, streamIdsToReference.keySet());
                StreamTestStreamIdxTable index = tables.getStreamTestStreamIdxTable(t);
                for (Map.Entry<Long, byte[]> entry : streamIdsToReference.entrySet()) {
                    Long streamId = entry.getKey();
                    byte[] reference = entry.getValue();
                    StreamTestStreamIdxTable.StreamTestStreamIdxColumn col = StreamTestStreamIdxTable.StreamTestStreamIdxColumn.of(reference);
                    StreamTestStreamIdxTable.StreamTestStreamIdxColumnValue value = StreamTestStreamIdxTable.StreamTestStreamIdxColumnValue.of(col, 0L);
                    index.put(StreamTestStreamIdxTable.StreamTestStreamIdxRow.of(streamId), value);
                }
                return null;
            }
        });
    }

    @Override
    public void removeStreamsAsUsed(final Map<Long, byte[]> streamIdsToReference) {
        if (streamIdsToReference.isEmpty()) {
            return;
        }
        txRunner.run(new TxTask() {
            @Override
            public Void execute(Transaction t) {
                StreamTestStreamIdxTable index = tables.getStreamTestStreamIdxTable(t);
                Multimap<StreamTestStreamIdxTable.StreamTestStreamIdxRow, StreamTestStreamIdxTable.StreamTestStreamIdxColumn> toDelete = ArrayListMultimap.create(streamIdsToReference.size(), 1);
                for (Map.Entry<Long, byte[]> entry : streamIdsToReference.entrySet()) {
                    Long streamId = entry.getKey();
                    byte[] reference = entry.getValue();
                    StreamTestStreamIdxTable.StreamTestStreamIdxColumn col = StreamTestStreamIdxTable.StreamTestStreamIdxColumn.of(reference);
                    toDelete.put(StreamTestStreamIdxTable.StreamTestStreamIdxRow.of(streamId), col);
                }
                index.delete(toDelete);
                Collection<StreamTestStreamIdxTable.StreamTestStreamIdxRow> rows = Collections2.transform(streamIdsToReference.keySet(), StreamTestStreamIdxTable.StreamTestStreamIdxRow.fromIdFun());
                Set<Long> streamsToKeep = ImmutableSet.copyOf(Collections2.transform(index.getRowsMultimap(rows).keySet(), StreamTestStreamIdxTable.StreamTestStreamIdxRow.getIdFun()));
                StreamTestStreamStore.of(t, tables).deleteStreams(Sets.difference(streamIdsToReference.keySet(), streamsToKeep));
                return null;
            }
        });
    }

    private void touchMetadataWhileMarkingUsedForConflicts(Transaction t, Iterable<Long> ids) {
        StreamTestStreamMetadataTable metaTable = tables.getStreamTestStreamMetadataTable(t);
        Set<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow> rows = Sets.newHashSet();
        for (Long id : ids) {
            rows.add(StreamTestStreamMetadataTable.StreamTestStreamMetadataRow.of(id));
        }
        Map<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow, StreamMetadata> metadatas = metaTable.getMetadatas(rows);
        for (Map.Entry<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow, StreamMetadata> e : metadatas.entrySet()) {
            StreamMetadata metadata = e.getValue();
            Preconditions.checkState(metadata.getStatus() == Status.STORED,
                    "Stream: " + e.getKey().getId() + " has status: " + metadata.getStatus());
            metaTable.putMetadata(e.getKey(), metadata);
        }
        SetView<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow> missingRows = Sets.difference(rows, metadatas.keySet());
        if (!missingRows.isEmpty()) {
            throw new IllegalStateException("Missing metadata rows for:" + missingRows
                    + " rows: " + rows + " metadata: " + metadatas + " txn timestamp: " + t.getTimestamp());
        }
    }

    /**
     * This exists to avoid unused import warnings
     * {@link ArrayListMultimap}
     * {@link Arrays}
     * {@link AssertUtils}
     * {@link BufferedInputStream}
     * {@link Builder}
     * {@link ByteArrayIOStream}
     * {@link ByteArrayInputStream}
     * {@link ByteString}
     * {@link Cell}
     * {@link Collection}
     * {@link Collections2}
     * {@link ConcatenatedInputStream}
     * {@link CountingInputStream}
     * {@link DeleteOnCloseFileInputStream}
     * {@link DigestInputStream}
     * {@link Entry}
     * {@link ExpiringStreamStore}
     * {@link File}
     * {@link FileNotFoundException}
     * {@link FileOutputStream}
     * {@link FileUtils}
     * {@link HashMultimap}
     * {@link IOException}
     * {@link IOUtils}
     * {@link ImmutableMap}
     * {@link ImmutableSet}
     * {@link InputStream}
     * {@link Ints}
     * {@link Lists}
     * {@link Logger}
     * {@link LoggerFactory}
     * {@link Map}
     * {@link Maps}
     * {@link MessageDigest}
     * {@link Multimap}
     * {@link Multimaps}
     * {@link OutputStream}
     * {@link PersistentStreamStore}
     * {@link Preconditions}
     * {@link Set}
     * {@link SetView}
     * {@link Sets}
     * {@link Sha256Hash}
     * {@link Status}
     * {@link StreamMetadata}
     * {@link Throwables}
     * {@link TimeUnit}
     * {@link Transaction}
     * {@link TransactionBackedRunner}
     * {@link TransactionManager}
     * {@link TransactionManagerBackedRunner}
     * {@link TransactionRunner}
     * {@link TransactionTask}
     * {@link TxTask}
     */
    static final int dummy = 0;
}
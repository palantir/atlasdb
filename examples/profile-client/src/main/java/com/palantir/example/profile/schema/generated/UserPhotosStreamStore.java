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
package com.palantir.example.profile.schema.generated;

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
import com.palantir.atlasdb.stream.AbstractExpiringStreamStore;
import com.palantir.atlasdb.stream.AbstractPersistentStreamStore;
import com.palantir.atlasdb.stream.ExpiringStreamStore;
import com.palantir.atlasdb.stream.PersistentStreamStore;
import com.palantir.atlasdb.stream.StreamCleanedException;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
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


public final class UserPhotosStreamStore extends AbstractPersistentStreamStore {
    public static final int BLOCK_SIZE_IN_BYTES = 1000000; // 1MB. DO NOT CHANGE THIS WITHOUT AN UPGRADE TASK
    public static final int IN_MEMORY_THRESHOLD = 2097152; // streams under this size are kept in memory when loaded
    public static final String STREAM_FILE_PREFIX = "UserPhotos_stream_";
    public static final String STREAM_FILE_SUFFIX = ".tmp";

    private static final Logger log = LoggerFactory.getLogger(UserPhotosStreamStore.class);

    private final ProfileTableFactory tables;

    private UserPhotosStreamStore(TransactionManager txManager, ProfileTableFactory tables) {
        super(txManager);
        this.tables = tables;
    }

    public static UserPhotosStreamStore of(TransactionManager txManager, ProfileTableFactory tables) {
        return new UserPhotosStreamStore(txManager, tables);
    }

    /**
     * This should only be used by test code or as a performance optimization.
     */
    static UserPhotosStreamStore of(ProfileTableFactory tables) {
        return new UserPhotosStreamStore(null, tables);
    }

    @Override
    protected long getInMemoryThreshold() {
        return IN_MEMORY_THRESHOLD;
    }

    @Override
    protected void storeBlock(long id, long blockNumber, final byte[] block) {
        Preconditions.checkArgument(block.length <= BLOCK_SIZE_IN_BYTES, "Block to store in DB must be less than BLOCK_SIZE_IN_BYTES");
        Preconditions.checkNotNull(txnMgr);
        final UserPhotosStreamValueTable.UserPhotosStreamValueRow row = UserPhotosStreamValueTable.UserPhotosStreamValueRow.of(id, blockNumber);
        try {
            txnMgr.runTaskThrowOnConflict(new TransactionTask<Void, RuntimeException>() {
                @Override
                public Void execute(Transaction t) {
                    // Do a touch operation on this table to ensure we get a conflict if someone cleans it up.
                    touchMetadataWhileStoringForConflicts(t, row.getId(), row.getBlockId());
                    tables.getUserPhotosStreamValueTable(t).putValue(row, block);
                    return null;
                }
            });
        } catch (RuntimeException e) {
            log.error("Error storing block " + row.getBlockId() + " for stream id " + row.getId(), e);
            throw e;
        }
    }

    private void touchMetadataWhileStoringForConflicts(Transaction t, Long id, long blockNumber) {
        UserPhotosStreamMetadataTable metaTable = tables.getUserPhotosStreamMetadataTable(t);
        UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow row = UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow.of(id);
        StreamMetadata metadata = metaTable.getMetadatas(ImmutableSet.of(row)).values().iterator().next();
        Preconditions.checkState(metadata.getStatus() == Status.STORING, "This stream is being cleaned up while storing blocks: " + id);
        Builder builder = StreamMetadata.newBuilder(metadata);
        builder.setLength(blockNumber * BLOCK_SIZE_IN_BYTES + 1);
        metaTable.putMetadata(row, builder.build());
    }

    @Override
    protected void putMetadataAndHashIndexTask(Transaction t, long streamId, StreamMetadata metadata) {
        UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow row = UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow.of(streamId);
        UserPhotosStreamMetadataTable mdTable = tables.getUserPhotosStreamMetadataTable(t);
        if (metadata.getStatus() == Status.STORED) {
            StreamMetadata prevMetadata = getMetadata(t, streamId);
            if (prevMetadata == null || prevMetadata.getStatus() != Status.STORING) {
                // This can happen if we cleanup old streams.
                throw new TransactionFailedRetriableException("Cannot mark a stream as stored that isn't currently storing: " + prevMetadata);
            }
            putHashIndexTask(t, row, metadata);
        } else if (metadata.getStatus() == Status.STORING) {
            StreamMetadata prevMetadata = getMetadata(t, streamId);
            // This will prevent two users trying to store the same id.
            if (prevMetadata != null) {
                throw new TransactionFailedRetriableException("Cannot reuse the same stream id: " + streamId);
            }
        }

        mdTable.putMetadata(row, metadata);
    }

    private long getNumberOfBlocksFromMetadata(StreamMetadata metadata) {
        return (metadata.getLength() + BLOCK_SIZE_IN_BYTES - 1) / BLOCK_SIZE_IN_BYTES;
    }

    @Override
    protected File createTempFile(Long id) throws IOException {
        File file = FileUtils.createTempFile(STREAM_FILE_PREFIX + id, STREAM_FILE_SUFFIX);
        file.deleteOnExit();
        return file;
    }

    @Override
    protected void loadSingleBlockToOutputStream(Transaction t, Long streamId, long blockId, OutputStream os) {
        UserPhotosStreamValueTable.UserPhotosStreamValueRow row = UserPhotosStreamValueTable.UserPhotosStreamValueRow.of(streamId, blockId);
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

    private byte[] getBlock(Transaction t, UserPhotosStreamValueTable.UserPhotosStreamValueRow row) {
        UserPhotosStreamValueTable valueTable = tables.getUserPhotosStreamValueTable(t);
        return valueTable.getValues(ImmutableSet.of(row)).get(row);
    }

    @Override
    protected StreamMetadata getMetadata(Transaction t, Long streamId) {
        UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow row = UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow.of(streamId);
        UserPhotosStreamMetadataTable table = tables.getUserPhotosStreamMetadataTable(t);
        return table.getMetadatas(ImmutableSet.of(row)).get(row);
    }

    @Override
    @CheckForNull
    public Long lookupStreamIdByHash(Transaction t, Sha256Hash hash) {
        UserPhotosStreamHashAidxTable idx = tables.getUserPhotosStreamHashAidxTable(t);
        List<UserPhotosStreamHashAidxTable.UserPhotosStreamHashAidxColumnValue> columns = idx.getRowColumns(UserPhotosStreamHashAidxTable.UserPhotosStreamHashAidxRow.of(hash));
        for (UserPhotosStreamHashAidxTable.UserPhotosStreamHashAidxColumnValue colVal : columns) {
            long streamId = colVal.getColumnName().getStreamId();
            StreamMetadata meta = getMetadata(t, streamId);
            if (meta.getStatus() == Status.STORED) {
                return streamId;
            }
        }
        return null;
    }

    private void putHashIndexTask(Transaction t, UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow row, StreamMetadata metadata) {
        Preconditions.checkArgument(
                metadata.getStatus() == Status.STORED,
                "Should only index successfully stored streams.");

        Sha256Hash hash = Sha256Hash.EMPTY;
        if (metadata.getHash() != com.google.protobuf.ByteString.EMPTY) {
            hash = new Sha256Hash(metadata.getHash().toByteArray());
        }
        UserPhotosStreamHashAidxTable.UserPhotosStreamHashAidxRow hashRow = UserPhotosStreamHashAidxTable.UserPhotosStreamHashAidxRow.of(hash);
        UserPhotosStreamHashAidxTable.UserPhotosStreamHashAidxColumn column = UserPhotosStreamHashAidxTable.UserPhotosStreamHashAidxColumn.of(row.getId());
        UserPhotosStreamHashAidxTable.UserPhotosStreamHashAidxColumnValue columnValue = UserPhotosStreamHashAidxTable.UserPhotosStreamHashAidxColumnValue.of(column, 0L);
        UserPhotosStreamHashAidxTable hiTable = tables.getUserPhotosStreamHashAidxTable(t);
        hiTable.put(hashRow, columnValue);
    }

    /**
     * This should only be used from the cleanup tasks.
     */
    void deleteStreams(Transaction t, final Set<Long> streamIds) {
        if (streamIds.isEmpty()) {
            return;
        }
        Set<UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow> smRows = Sets.newHashSet();
        Multimap<UserPhotosStreamHashAidxTable.UserPhotosStreamHashAidxRow, UserPhotosStreamHashAidxTable.UserPhotosStreamHashAidxColumn> shToDelete = HashMultimap.create();
        for (Long streamId : streamIds) {
            smRows.add(UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow.of(streamId));
        }
        UserPhotosStreamMetadataTable table = tables.getUserPhotosStreamMetadataTable(t);
        Map<UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow, StreamMetadata> metadatas = table.getMetadatas(smRows);
        Set<UserPhotosStreamValueTable.UserPhotosStreamValueRow> streamValueToDelete = Sets.newHashSet();
        for (Entry<UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow, StreamMetadata> e : metadatas.entrySet()) {
            Long streamId = e.getKey().getId();
            long blocks = getNumberOfBlocksFromMetadata(e.getValue());
            for (long i = 0; i < blocks; i++) {
                streamValueToDelete.add(UserPhotosStreamValueTable.UserPhotosStreamValueRow.of(streamId, i));
            }
            ByteString streamHash = e.getValue().getHash();
            Sha256Hash hash = Sha256Hash.EMPTY;
            if (streamHash != com.google.protobuf.ByteString.EMPTY) {
                hash = new Sha256Hash(streamHash.toByteArray());
            } else {
                log.error("Empty hash for stream " + streamId);
            }
            UserPhotosStreamHashAidxTable.UserPhotosStreamHashAidxRow hashRow = UserPhotosStreamHashAidxTable.UserPhotosStreamHashAidxRow.of(hash);
            UserPhotosStreamHashAidxTable.UserPhotosStreamHashAidxColumn column = UserPhotosStreamHashAidxTable.UserPhotosStreamHashAidxColumn.of(streamId);
            shToDelete.put(hashRow, column);
        }
        tables.getUserPhotosStreamHashAidxTable(t).delete(shToDelete);
        tables.getUserPhotosStreamValueTable(t).delete(streamValueToDelete);
        table.delete(smRows);
    }

    @Override
    protected void markStreamAsUsedInternal(Transaction t, long streamId, byte[] reference) {
        UserPhotosStreamIdxTable index = tables.getUserPhotosStreamIdxTable(t);
        UserPhotosStreamIdxTable.UserPhotosStreamIdxColumn col = UserPhotosStreamIdxTable.UserPhotosStreamIdxColumn.of(reference);
        UserPhotosStreamIdxTable.UserPhotosStreamIdxColumnValue value = UserPhotosStreamIdxTable.UserPhotosStreamIdxColumnValue.of(col, 0L);
        index.put(UserPhotosStreamIdxTable.UserPhotosStreamIdxRow.of(streamId), value);
    }

    @Override
    public void unmarkStreamAsUsed(Transaction t, long streamId, byte[] reference) {
        UserPhotosStreamIdxTable index = tables.getUserPhotosStreamIdxTable(t);
        UserPhotosStreamIdxTable.UserPhotosStreamIdxRow row = UserPhotosStreamIdxTable.UserPhotosStreamIdxRow.of(streamId);
        UserPhotosStreamIdxTable.UserPhotosStreamIdxColumn col = UserPhotosStreamIdxTable.UserPhotosStreamIdxColumn.of(reference);
        index.delete(row, col);
    }

    @Override
    protected void touchMetadataWhileMarkingUsedForConflicts(Transaction t, long streamId) {
        UserPhotosStreamMetadataTable metaTable = tables.getUserPhotosStreamMetadataTable(t);
        Set<UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow> rows = Sets.newHashSet();
        rows.add(UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow.of(streamId));
        Map<UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow, StreamMetadata> metadatas = metaTable.getMetadatas(rows);
        for (Map.Entry<UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow, StreamMetadata> e : metadatas.entrySet()) {
            StreamMetadata metadata = e.getValue();
            Preconditions.checkState(metadata.getStatus() == Status.STORED,
                    "Stream: " + e.getKey().getId() + " has status: " + metadata.getStatus());
            metaTable.putMetadata(e.getKey(), metadata);
        }
        SetView<UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow> missingRows = Sets.difference(rows, metadatas.keySet());
        if (!missingRows.isEmpty()) {
            throw new StreamCleanedException("Missing metadata rows for:" + missingRows
                    + " rows: " + rows + " metadata: " + metadatas + " txn timestamp: " + t.getTimestamp());
        }
    }

    /**
     * This exists to avoid unused import warnings
     * {@link AbstractExpiringStreamStore}
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
     * {@link ExpiringStreamStore}
     * {@link File}
     * {@link FileNotFoundException}
     * {@link FileOutputStream}
     * {@link FileUtils}
     * {@link HashMultimap}
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
     * {@link OutputStream}
     * {@link PersistentStreamStore}
     * {@link Preconditions}
     * {@link Set}
     * {@link SetView}
     * {@link Sets}
     * {@link Sha256Hash}
     * {@link Status}
     * {@link StreamCleanedException}
     * {@link StreamMetadata}
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

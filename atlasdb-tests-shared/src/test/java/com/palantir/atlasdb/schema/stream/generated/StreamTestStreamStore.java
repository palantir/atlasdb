package com.palantir.atlasdb.schema.stream.generated;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Generated;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.google.protobuf.ByteString;
import com.palantir.atlasdb.protos.generated.StreamPersistence.Status;
import com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata;
import com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata.Builder;
import com.palantir.atlasdb.stream.AbstractPersistentStreamStore;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.common.base.Throwables;
import com.palantir.util.AssertUtils;
import com.palantir.util.crypto.Sha256Hash;
import com.palantir.util.file.TempFileUtils;

@Generated("com.palantir.atlasdb.table.description.render.StreamStoreRenderer")
public final class StreamTestStreamStore extends AbstractPersistentStreamStore {
  public static final int BLOCK_SIZE_IN_BYTES =
      1000000; // 1MB. DO NOT CHANGE THIS WITHOUT AN UPGRADE TASK
  public static final int IN_MEMORY_THRESHOLD =
      4194304; // streams under this size are kept in memory when loaded
  public static final String STREAM_FILE_PREFIX = "StreamTest_stream_";
  public static final String STREAM_FILE_SUFFIX = ".tmp";

  private static final Logger log = LoggerFactory.getLogger(StreamTestStreamStore.class);

  private final StreamTestTableFactory tables;

  private StreamTestStreamStore(TransactionManager txManager, StreamTestTableFactory tables) {
    super(txManager);
    this.tables = tables;
  }

  public static StreamTestStreamStore of(
      TransactionManager txManager, StreamTestTableFactory tables) {
    return new StreamTestStreamStore(txManager, tables);
  }

  /** This should only be used by test code or as a performance optimization. */
  static StreamTestStreamStore of(StreamTestTableFactory tables) {
    return new StreamTestStreamStore(null, tables);
  }

  @Override
  protected long getInMemoryThreshold() {
    return IN_MEMORY_THRESHOLD;
  }

  @Override
  protected void storeBlock(Transaction t, long id, long blockNumber, final byte[] block) {
    Preconditions.checkArgument(
        block.length <= BLOCK_SIZE_IN_BYTES,
        "Block to store in DB must be less than BLOCK_SIZE_IN_BYTES");
    final StreamTestStreamValueTable.StreamTestStreamValueRow row =
        StreamTestStreamValueTable.StreamTestStreamValueRow.of(id, blockNumber);
    try {
      // Do a touch operation on this table to ensure we get a conflict if someone cleans it up.
      touchMetadataWhileStoringForConflicts(t, row.getId(), row.getBlockId());
      tables.getStreamTestStreamValueTable(t).putValue(row, block);
    } catch (RuntimeException e) {
      log.error("Error storing block " + row.getBlockId() + " for stream id " + row.getId(), e);
      throw e;
    }
  }

  private void touchMetadataWhileStoringForConflicts(Transaction t, Long id, long blockNumber) {
    StreamTestStreamMetadataTable metaTable = tables.getStreamTestStreamMetadataTable(t);
    StreamTestStreamMetadataTable.StreamTestStreamMetadataRow row =
        StreamTestStreamMetadataTable.StreamTestStreamMetadataRow.of(id);
    StreamMetadata metadata =
        metaTable.getMetadatas(ImmutableSet.of(row)).values().iterator().next();
    Preconditions.checkState(
        metadata.getStatus() == Status.STORING,
        "This stream is being cleaned up while storing blocks: " + id);
    Builder builder = StreamMetadata.newBuilder(metadata);
    builder.setLength(blockNumber * BLOCK_SIZE_IN_BYTES + 1);
    metaTable.putMetadata(row, builder.build());
  }

  @Override
  protected void putMetadataAndHashIndexTask(
      Transaction t, Map<Long, StreamMetadata> streamIdsToMetadata) {
    StreamTestStreamMetadataTable mdTable = tables.getStreamTestStreamMetadataTable(t);
    Map<Long, StreamMetadata> prevMetadatas = getMetadata(t, streamIdsToMetadata.keySet());

    Map<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow, StreamMetadata>
        rowsToStoredMetadata = Maps.newHashMap();
    Map<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow, StreamMetadata>
        rowsToUnstoredMetadata = Maps.newHashMap();
    for (Entry<Long, StreamMetadata> e : streamIdsToMetadata.entrySet()) {
      long streamId = e.getKey();
      StreamMetadata metadata = e.getValue();
      StreamMetadata prevMetadata = prevMetadatas.get(streamId);
      if (metadata.getStatus() == Status.STORED) {
        if (prevMetadata == null || prevMetadata.getStatus() != Status.STORING) {
          // This can happen if we cleanup old streams.
          throw new TransactionFailedRetriableException(
              "Cannot mark a stream as stored that isn't currently storing: " + prevMetadata);
        }
        rowsToStoredMetadata.put(
            StreamTestStreamMetadataTable.StreamTestStreamMetadataRow.of(streamId), metadata);
      } else if (metadata.getStatus() == Status.STORING) {
        // This will prevent two users trying to store the same id.
        if (prevMetadata != null) {
          throw new TransactionFailedRetriableException(
              "Cannot reuse the same stream id: " + streamId);
        }
        rowsToUnstoredMetadata.put(
            StreamTestStreamMetadataTable.StreamTestStreamMetadataRow.of(streamId), metadata);
      }
    }
    putHashIndexTask(t, rowsToStoredMetadata);

    Map<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow, StreamMetadata> rowsToMetadata =
        Maps.newHashMap();
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
  protected void loadSingleBlockToOutputStream(
      Transaction t, Long streamId, long blockId, OutputStream os) {
    StreamTestStreamValueTable.StreamTestStreamValueRow row =
        StreamTestStreamValueTable.StreamTestStreamValueRow.of(streamId, blockId);
    try {
      os.write(getBlock(t, row));
    } catch (RuntimeException e) {
      log.error("Error getting block " + row.getBlockId() + " of stream " + row.getId(), e);
      throw e;
    } catch (IOException e) {
      log.error(
          "Error writing block "
              + row.getBlockId()
              + " to file when getting stream id "
              + row.getId(),
          e);
      throw Throwables.rewrapAndThrowUncheckedException(
          "Error writing blocks to file when creating stream.", e);
    }
  }

  private byte[] getBlock(Transaction t, StreamTestStreamValueTable.StreamTestStreamValueRow row) {
    StreamTestStreamValueTable valueTable = tables.getStreamTestStreamValueTable(t);
    return valueTable.getValues(ImmutableSet.of(row)).get(row);
  }

  @Override
  protected Map<Long, StreamMetadata> getMetadata(Transaction t, Set<Long> streamIds) {
    if (streamIds.isEmpty()) {
      return ImmutableMap.of();
    }
    StreamTestStreamMetadataTable table = tables.getStreamTestStreamMetadataTable(t);
    Map<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow, StreamMetadata> metadatas =
        table.getMetadatas(getMetadataRowsForIds(streamIds));
    Map<Long, StreamMetadata> ret = Maps.newHashMap();
    for (Map.Entry<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow, StreamMetadata> e :
        metadatas.entrySet()) {
      ret.put(e.getKey().getId(), e.getValue());
    }
    return ret;
  }

  @Override
  public Map<Sha256Hash, Long> lookupStreamIdsByHash(Transaction t, final Set<Sha256Hash> hashes) {
    if (hashes.isEmpty()) {
      return ImmutableMap.of();
    }
    StreamTestStreamHashAidxTable idx = tables.getStreamTestStreamHashAidxTable(t);
    Set<StreamTestStreamHashAidxTable.StreamTestStreamHashAidxRow> rows =
        getHashIndexRowsForHashes(hashes);

    Multimap<
            StreamTestStreamHashAidxTable.StreamTestStreamHashAidxRow,
            StreamTestStreamHashAidxTable.StreamTestStreamHashAidxColumnValue>
        m = idx.getRowsMultimap(rows);
    Map<Long, Sha256Hash> hashForStreams = Maps.newHashMap();
    for (StreamTestStreamHashAidxTable.StreamTestStreamHashAidxRow r : m.keySet()) {
      for (StreamTestStreamHashAidxTable.StreamTestStreamHashAidxColumnValue v : m.get(r)) {
        Long streamId = v.getColumnName().getStreamId();
        Sha256Hash hash = r.getHash();
        if (hashForStreams.containsKey(streamId)) {
          AssertUtils.assertAndLog(
              hashForStreams.get(streamId).equals(hash),
              "(BUG) Stream ID has 2 different hashes: " + streamId);
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

  private Set<StreamTestStreamHashAidxTable.StreamTestStreamHashAidxRow> getHashIndexRowsForHashes(
      final Set<Sha256Hash> hashes) {
    Set<StreamTestStreamHashAidxTable.StreamTestStreamHashAidxRow> rows = Sets.newHashSet();
    for (Sha256Hash h : hashes) {
      rows.add(StreamTestStreamHashAidxTable.StreamTestStreamHashAidxRow.of(h));
    }
    return rows;
  }

  private Set<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow> getMetadataRowsForIds(
      final Iterable<Long> ids) {
    Set<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow> rows = Sets.newHashSet();
    for (Long id : ids) {
      rows.add(StreamTestStreamMetadataTable.StreamTestStreamMetadataRow.of(id));
    }
    return rows;
  }

  private void putHashIndexTask(
      Transaction t,
      Map<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow, StreamMetadata>
          rowsToMetadata) {
    Multimap<
            StreamTestStreamHashAidxTable.StreamTestStreamHashAidxRow,
            StreamTestStreamHashAidxTable.StreamTestStreamHashAidxColumnValue>
        indexMap = HashMultimap.create();
    for (Entry<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow, StreamMetadata> e :
        rowsToMetadata.entrySet()) {
      StreamTestStreamMetadataTable.StreamTestStreamMetadataRow row = e.getKey();
      StreamMetadata metadata = e.getValue();
      Preconditions.checkArgument(
          metadata.getStatus() == Status.STORED, "Should only index successfully stored streams.");

      Sha256Hash hash = Sha256Hash.EMPTY;
      if (metadata.getHash() != com.google.protobuf.ByteString.EMPTY) {
        hash = new Sha256Hash(metadata.getHash().toByteArray());
      }
      StreamTestStreamHashAidxTable.StreamTestStreamHashAidxRow hashRow =
          StreamTestStreamHashAidxTable.StreamTestStreamHashAidxRow.of(hash);
      StreamTestStreamHashAidxTable.StreamTestStreamHashAidxColumn column =
          StreamTestStreamHashAidxTable.StreamTestStreamHashAidxColumn.of(row.getId());
      StreamTestStreamHashAidxTable.StreamTestStreamHashAidxColumnValue columnValue =
          StreamTestStreamHashAidxTable.StreamTestStreamHashAidxColumnValue.of(column, 0L);
      indexMap.put(hashRow, columnValue);
    }
    StreamTestStreamHashAidxTable hiTable = tables.getStreamTestStreamHashAidxTable(t);
    hiTable.put(indexMap);
  }

  /** This should only be used from the cleanup tasks. */
  void deleteStreams(Transaction t, final Set<Long> streamIds) {
    if (streamIds.isEmpty()) {
      return;
    }
    Set<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow> smRows = Sets.newHashSet();
    Multimap<
            StreamTestStreamHashAidxTable.StreamTestStreamHashAidxRow,
            StreamTestStreamHashAidxTable.StreamTestStreamHashAidxColumn>
        shToDelete = HashMultimap.create();
    for (Long streamId : streamIds) {
      smRows.add(StreamTestStreamMetadataTable.StreamTestStreamMetadataRow.of(streamId));
    }
    StreamTestStreamMetadataTable table = tables.getStreamTestStreamMetadataTable(t);
    Map<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow, StreamMetadata> metadatas =
        table.getMetadatas(smRows);
    Set<StreamTestStreamValueTable.StreamTestStreamValueRow> streamValueToDelete =
        Sets.newHashSet();
    for (Entry<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow, StreamMetadata> e :
        metadatas.entrySet()) {
      Long streamId = e.getKey().getId();
      long blocks = getNumberOfBlocksFromMetadata(e.getValue());
      for (long i = 0; i < blocks; i++) {
        streamValueToDelete.add(
            StreamTestStreamValueTable.StreamTestStreamValueRow.of(streamId, i));
      }
      ByteString streamHash = e.getValue().getHash();
      Sha256Hash hash = Sha256Hash.EMPTY;
      if (streamHash != com.google.protobuf.ByteString.EMPTY) {
        hash = new Sha256Hash(streamHash.toByteArray());
      } else {
        log.error("Empty hash for stream " + streamId);
      }
      StreamTestStreamHashAidxTable.StreamTestStreamHashAidxRow hashRow =
          StreamTestStreamHashAidxTable.StreamTestStreamHashAidxRow.of(hash);
      StreamTestStreamHashAidxTable.StreamTestStreamHashAidxColumn column =
          StreamTestStreamHashAidxTable.StreamTestStreamHashAidxColumn.of(streamId);
      shToDelete.put(hashRow, column);
    }
    tables.getStreamTestStreamHashAidxTable(t).delete(shToDelete);
    tables.getStreamTestStreamValueTable(t).delete(streamValueToDelete);
    table.delete(smRows);
  }

  @Override
  protected void markStreamsAsUsedInternal(
      Transaction t, final Map<Long, byte[]> streamIdsToReference) {
    if (streamIdsToReference.isEmpty()) {
      return;
    }
    StreamTestStreamIdxTable index = tables.getStreamTestStreamIdxTable(t);
    Multimap<
            StreamTestStreamIdxTable.StreamTestStreamIdxRow,
            StreamTestStreamIdxTable.StreamTestStreamIdxColumnValue>
        rowsToValues = HashMultimap.create();
    for (Map.Entry<Long, byte[]> entry : streamIdsToReference.entrySet()) {
      Long streamId = entry.getKey();
      byte[] reference = entry.getValue();
      StreamTestStreamIdxTable.StreamTestStreamIdxColumn col =
          StreamTestStreamIdxTable.StreamTestStreamIdxColumn.of(reference);
      StreamTestStreamIdxTable.StreamTestStreamIdxColumnValue value =
          StreamTestStreamIdxTable.StreamTestStreamIdxColumnValue.of(col, 0L);
      rowsToValues.put(StreamTestStreamIdxTable.StreamTestStreamIdxRow.of(streamId), value);
    }
    index.put(rowsToValues);
  }

  @Override
  public void unmarkStreamsAsUsed(Transaction t, final Map<Long, byte[]> streamIdsToReference) {
    if (streamIdsToReference.isEmpty()) {
      return;
    }
    StreamTestStreamIdxTable index = tables.getStreamTestStreamIdxTable(t);
    Multimap<
            StreamTestStreamIdxTable.StreamTestStreamIdxRow,
            StreamTestStreamIdxTable.StreamTestStreamIdxColumn>
        toDelete = ArrayListMultimap.create(streamIdsToReference.size(), 1);
    for (Map.Entry<Long, byte[]> entry : streamIdsToReference.entrySet()) {
      Long streamId = entry.getKey();
      byte[] reference = entry.getValue();
      StreamTestStreamIdxTable.StreamTestStreamIdxColumn col =
          StreamTestStreamIdxTable.StreamTestStreamIdxColumn.of(reference);
      toDelete.put(StreamTestStreamIdxTable.StreamTestStreamIdxRow.of(streamId), col);
    }
    index.delete(toDelete);
  }

  @Override
  protected void touchMetadataWhileMarkingUsedForConflicts(Transaction t, Iterable<Long> ids) {
    StreamTestStreamMetadataTable metaTable = tables.getStreamTestStreamMetadataTable(t);
    Set<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow> rows = Sets.newHashSet();
    for (Long id : ids) {
      rows.add(StreamTestStreamMetadataTable.StreamTestStreamMetadataRow.of(id));
    }
    Map<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow, StreamMetadata> metadatas =
        metaTable.getMetadatas(rows);
    for (Map.Entry<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow, StreamMetadata> e :
        metadatas.entrySet()) {
      StreamMetadata metadata = e.getValue();
      Preconditions.checkState(
          metadata.getStatus() == Status.STORED,
          "Stream: " + e.getKey().getId() + " has status: " + metadata.getStatus());
      metaTable.putMetadata(e.getKey(), metadata);
    }
    SetView<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow> missingRows =
        Sets.difference(rows, metadatas.keySet());
    if (!missingRows.isEmpty()) {
      throw new IllegalStateException(
          "Missing metadata rows for:"
              + missingRows
              + " rows: "
              + rows
              + " metadata: "
              + metadatas
              + " txn timestamp: "
              + t.getTimestamp());
    }
  }

  static final int dummy = 0;
}

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
package com.palantir.atlasdb.stream;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.Map;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.common.io.CountingInputStream;
import com.google.protobuf.ByteString;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.protos.generated.StreamPersistence.Status;
import com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.atlasdb.transaction.impl.TxTask;
import com.palantir.common.base.Throwables;
import com.palantir.util.Pair;
import com.palantir.util.crypto.Sha256Hash;

public abstract class AbstractPersistentStreamStore extends AbstractGenericStreamStore<Long> implements PersistentStreamStore {
    protected AbstractPersistentStreamStore(TransactionManager txManager) {
        super(txManager);
    }

    private final void storeMetadataAndIndex(final long streamId, final StreamMetadata metadata){
        Preconditions.checkNotNull(txnMgr);
        txnMgr.runTaskThrowOnConflict(new TxTask() {
            @Override
            public Void execute(Transaction t) {
                putMetadataAndHashIndexTask(t, streamId, metadata);
                return null;
            }
        });
    }

    private Long lookupStreamIdByHash(Transaction t, Sha256Hash hash) {
        Map<Sha256Hash, Long> hashToId = lookupStreamIdsByHash(t, Sets.newHashSet(hash));
        if (hashToId.isEmpty()) {
           return null;
        }
        return Iterables.getOnlyElement(hashToId.entrySet()).getValue();
    }

    @Override
    public final long getByHashOrStoreStreamAndMarkAsUsed(Transaction t, Sha256Hash hash, InputStream stream, byte[] reference) {
        Long streamId = lookupStreamIdByHash(t, hash);
        if (streamId != null) {
            markStreamsAsUsed(t, ImmutableMap.<Long, byte[]>builder().put(streamId, reference).build());
            return streamId;
        }
        Pair<Long, Sha256Hash> pair = storeStream(stream);
        Preconditions.checkArgument(hash.equals(pair.rhSide), "passed hash: %s does not equal stream hash: %s", hash, pair.rhSide);
        markStreamsAsUsedInternal(t, ImmutableMap.<Long, byte[]>builder().put(pair.lhSide, reference).build());
        return pair.lhSide;
    }

    @Override
    public void unmarkStreamAsUsed(Transaction t, long streamId, byte[] reference) {
        unmarkStreamsAsUsed(t, ImmutableMap.<Long, byte[]>builder().put(streamId, reference).build());
    }

    @Override
    public void markStreamAsUsed(Transaction t, long streamId, byte[] reference) {
        markStreamsAsUsed(t, ImmutableMap.<Long, byte[]>builder().put(streamId, reference).build());
    }

    @Override
    public void markStreamsAsUsed(Transaction t, Map<Long, byte[]> streamIdsToReference) {
        touchMetadataWhileMarkingUsedForConflicts(t, streamIdsToReference.keySet());
        markStreamsAsUsedInternal(t, streamIdsToReference);
    }

    private long storeEmptyMetadata() {
        Preconditions.checkNotNull(txnMgr);
        return txnMgr.runTaskThrowOnConflict(new TransactionTask<Long, RuntimeException>() {
            @Override
            public Long execute(Transaction t) {
                putMetadataAndHashIndexTask(t, t.getTimestamp(), getEmptyMetadata());
                return t.getTimestamp();
            }
        });
    }

    @Override
    public final Pair<Long, Sha256Hash> storeStream(InputStream stream) {
        // Store empty metadata before doing anything
        long id = storeEmptyMetadata();

        StreamMetadata metadata = storeBlocksAndGetFinalMetadata(null, id, stream);
        storeMetadataAndIndex(id, metadata);
        return Pair.create(id, new Sha256Hash(metadata.getHash().toByteArray()));
    }

    @Override
    public Map<Long, Sha256Hash> storeStreams(final Transaction t, final Map<Long, InputStream> streams) {
        if (streams.isEmpty()) {
            return ImmutableMap.of();
        }

        Map<Long, StreamMetadata> idsToEmptyMetadata = Maps.transformValues(streams, Functions.constant(getEmptyMetadata()));
        putMetadataAndHashIndexTask(t, idsToEmptyMetadata);

        Map<Long, StreamMetadata> idsToMetadata = Maps.transformEntries(streams, new Maps.EntryTransformer<Long, InputStream, StreamMetadata>() {
            @Override
            public StreamMetadata transformEntry(Long id, InputStream stream) {
                return storeBlocksAndGetFinalMetadata(t, id, stream);
            }
        });
        putMetadataAndHashIndexTask(t, idsToMetadata);

        Map<Long, Sha256Hash> hashes = Maps.transformValues(idsToMetadata, new Function<StreamMetadata, Sha256Hash>() {
            @Override
            public Sha256Hash apply(StreamMetadata metadata) {
                return new Sha256Hash(metadata.getHash().toByteArray());
            }
        });
        return hashes;
    }

    protected final StreamMetadata storeBlocksAndGetFinalMetadata(@Nullable Transaction t, long id, InputStream stream) {
        // Set up for finding hash and length
        MessageDigest digest = Sha256Hash.getMessageDigest();
        stream = new DigestInputStream(stream, digest);
        CountingInputStream countingStream = new CountingInputStream(stream);

        // Try to store the bytes to the stream and get length
        try {
            storeBlocksFromStream(t, id, countingStream);
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


    private void storeBlocksFromStream(@Nullable Transaction t, long id, InputStream stream) throws IOException {
        // We need to use a buffered stream here because we assume each read will fill the whole buffer.
        stream = new BufferedInputStream(stream);
        byte[] bytesToStore = new byte[BLOCK_SIZE_IN_BYTES];
        long blockNumber = 0;

        while (true) {
            int length = ByteStreams.read(stream, bytesToStore, 0, BLOCK_SIZE_IN_BYTES);
            // Store only relevant data if it only filled a partial block
            if (length == 0) {
                break;
            }
            if (length < BLOCK_SIZE_IN_BYTES) {
                // This is the last block.
                storeBlockWithNonNullTransaction(t, id, blockNumber, PtBytes.head(bytesToStore, length));
                break;
            } else {
                // Store a full block.
                storeBlockWithNonNullTransaction(t, id, blockNumber, bytesToStore);
            }
            blockNumber++;
        }
    }

    protected void storeBlockWithNonNullTransaction(@Nullable Transaction t, final long id, final long blockNumber, final byte[] bytesToStore) {
        if (t != null) {
            storeBlock(t, id, blockNumber, bytesToStore);
        } else {
            Preconditions.checkNotNull(txnMgr);
            txnMgr.runTaskThrowOnConflict(
                    new TransactionTask<Void, RuntimeException>() {
                        @Override
                        public Void execute(Transaction t) throws RuntimeException {
                            storeBlock(t, id, blockNumber, bytesToStore);
                            return null;
                        }
                    });
        }
    }

    private void putMetadataAndHashIndexTask(Transaction t, Long streamId, StreamMetadata metadata) {
        putMetadataAndHashIndexTask(t, ImmutableMap.<Long, StreamMetadata>builder().put(streamId, metadata).build());
    }

    protected abstract void storeBlock(Transaction t, long id, long blockNumber, byte[] block);

    protected abstract void touchMetadataWhileMarkingUsedForConflicts(Transaction t, Iterable<Long> ids) throws StreamCleanedException;

    protected abstract void markStreamsAsUsedInternal(Transaction t, final Map<Long, byte[]> streamIdsToReference);

    protected abstract void putMetadataAndHashIndexTask(Transaction t, Map<Long, StreamMetadata> streamIdsToMetadata);
}

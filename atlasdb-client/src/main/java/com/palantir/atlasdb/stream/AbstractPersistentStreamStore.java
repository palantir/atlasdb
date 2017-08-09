/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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

import java.io.IOException;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.Map;

import javax.annotation.Nullable;

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

    protected final void storeMetadataAndIndex(final long streamId, final StreamMetadata metadata) {
        Preconditions.checkNotNull(txnMgr);
        txnMgr.runTaskThrowOnConflict((TxTask) t -> {
            putMetadataAndHashIndexTask(t, streamId, metadata);
            return null;
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

    protected long storeEmptyMetadata() {
        Preconditions.checkNotNull(txnMgr);
        return txnMgr.runTaskThrowOnConflict(t -> {
            putMetadataAndHashIndexTask(t, t.getTimestamp(), getEmptyMetadata());
            return t.getTimestamp();
        });
    }

    @Override
    public Pair<Long, Sha256Hash> storeStream(InputStream stream) {
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

        Map<Long, StreamMetadata> idsToMetadata = Maps.transformEntries(streams,
                (id, stream) -> storeBlocksAndGetFinalMetadata(t, id, stream));
        putMetadataAndHashIndexTask(t, idsToMetadata);

        Map<Long, Sha256Hash> hashes = Maps.transformValues(idsToMetadata,
                metadata -> new Sha256Hash(metadata.getHash().toByteArray()));
        return hashes;
    }

    // This method is overridden in generated code. Changes to this method may have unintended consequences.
    protected StreamMetadata storeBlocksAndGetFinalMetadata(@Nullable Transaction t, long id, InputStream stream) {
        MessageDigest digest = Sha256Hash.getMessageDigest();
        try (InputStream hashingStream = new DigestInputStream(stream, digest)) {
            StreamMetadata metadata = storeBlocksAndGetHashlessMetadata(t, id, hashingStream);
            return StreamMetadata.newBuilder(metadata)
                    .setHash(ByteString.copyFrom(digest.digest()))
                    .build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected final StreamMetadata storeBlocksAndGetHashlessMetadata(@Nullable Transaction t, long id, InputStream stream) {
        CountingInputStream countingStream = new CountingInputStream(stream);

        // Try to store the bytes in the stream and get length
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
            log.error("Could not store stream {}. Failed after {} bytes.", id, length, e);
            throw Throwables.rewrapAndThrowUncheckedException("Failed to store stream.", e);
        }

        long length = countingStream.getCount();
        return StreamMetadata.newBuilder()
                .setStatus(Status.STORED)
                .setLength(length)
                .setHash(com.google.protobuf.ByteString.EMPTY)
                .build();
    }

    private void storeBlocksFromStream(@Nullable Transaction t, long id, InputStream stream) throws IOException {
        long blockNumber = 0;

        while (true) {
            byte[] bytesToStore = new byte[BLOCK_SIZE_IN_BYTES];
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
                    (TransactionTask<Void, RuntimeException>) t1 -> {
                        storeBlock(t1, id, blockNumber, bytesToStore);
                        return null;
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

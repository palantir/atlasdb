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

import com.google.common.base.Preconditions;
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

    private final void storeMetadataAndIndex(final long streamId, final StreamMetadata metadata) {
        Preconditions.checkNotNull(txnMgr);
        txnMgr.runTaskThrowOnConflict(new TxTask() {
            @Override
            public Void execute(Transaction t) {
                putMetadataAndHashIndexTask(t, streamId, metadata);
                return null;
            }
        });
    }

    @Override
    public final long getByHashOrStoreStreamAndMarkAsUsed(Transaction t,
                                                          Sha256Hash hash,
                                                          InputStream stream,
                                                          byte[] reference) {
        Long streamId = lookupStreamIdByHash(t, hash);
        if (streamId != null) {
            markStreamAsUsed(t, streamId, reference);
            return streamId;
        }
        Pair<Long, Sha256Hash> pair = storeStream(stream);
        Preconditions.checkArgument(hash.equals(pair.rhSide), "passed hash: %s does not equal stream hash: %s", hash, pair.rhSide);
        markStreamAsUsedInternal(t, pair.lhSide, reference);
        return pair.lhSide;
    }

    @Override
    public void markStreamAsUsed(Transaction t, long streamId, byte[] reference) {
        touchMetadataWhileMarkingUsedForConflicts(t, streamId);
        markStreamAsUsedInternal(t, streamId, reference);
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

        StreamMetadata metadata = storeBlocksAndGetFinalMetadata(id, stream);
        storeMetadataAndIndex(id, metadata);
        return Pair.create(id, new Sha256Hash(metadata.getHash().toByteArray()));
    }

    protected final StreamMetadata storeBlocksAndGetFinalMetadata(long id, InputStream stream) {
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


    private void storeBlocksFromStream(long id, InputStream stream) throws IOException {
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
                storeBlock(id, blockNumber, PtBytes.head(bytesToStore, length));
                break;
            } else {
                // Store a full block.
                storeBlock(id, blockNumber, bytesToStore);
            }
            blockNumber++;
        }
    }

    protected abstract void storeBlock(long id, long blockNumber, byte[] block);

    protected abstract void touchMetadataWhileMarkingUsedForConflicts(Transaction t, long streamId) throws StreamCleanedException;

    protected abstract void markStreamAsUsedInternal(Transaction t, long streamId, byte[] reference);

    protected abstract void putMetadataAndHashIndexTask(Transaction t, long streamId, StreamMetadata metadata);
}

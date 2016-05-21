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
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
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
import com.palantir.util.crypto.Sha256Hash;

public abstract class AbstractExpiringStreamStore<ID> extends AbstractGenericStreamStore<ID> implements ExpiringStreamStore<ID> {
    protected AbstractExpiringStreamStore(TransactionManager txManager) {
        super(txManager);
    }

    @Override
    public Sha256Hash storeStream(ID id, InputStream stream, long duration, TimeUnit durationUnit) {
        // Store empty metadata before doing anything
        storeEmptyMetadata(id, duration, durationUnit);

        StreamMetadata metadata = storeBlocksAndGetFinalMetadata(id, stream, duration, durationUnit);
        storeMetadataAndIndex(id, metadata, duration, durationUnit);
        return new Sha256Hash(metadata.getHash().toByteArray());
    }

    private final void storeMetadataAndIndex(final ID streamId, final StreamMetadata metadata, final long duration, final TimeUnit unit) {
        Preconditions.checkNotNull(txnMgr);
        txnMgr.runTaskThrowOnConflict(new TxTask() {
            @Override
            public Void execute(Transaction t) {
                putMetadataAndHashIndexTask(t, ImmutableMap.of(streamId, metadata), duration, unit);
                return null;
            }
        });
    }

    private long storeEmptyMetadata(final ID streamId, final long duration, final TimeUnit unit) {
        Preconditions.checkNotNull(txnMgr);
        return txnMgr.runTaskThrowOnConflict(new TransactionTask<Long, RuntimeException>() {
            @Override
            public Long execute(Transaction t) {
                putMetadataAndHashIndexTask(t, ImmutableMap.of(streamId, getEmptyMetadata()), duration, unit);
                return t.getTimestamp();
            }
        });
    }

    protected final StreamMetadata storeBlocksAndGetFinalMetadata(ID id, InputStream stream, long duration, TimeUnit durationUnit) {
        // Set up for finding hash and length
        MessageDigest digest = Sha256Hash.getMessageDigest();
        stream = new DigestInputStream(stream, digest);
        CountingInputStream countingStream = new CountingInputStream(stream);

        // Try to store the bytes to the stream and get length
        try {
            storeBlocksFromStream(id, countingStream, duration, durationUnit);
        } catch (IOException e) {
            long length = countingStream.getCount();
            StreamMetadata metadata = StreamMetadata.newBuilder()
                .setStatus(Status.FAILED)
                .setLength(length)
                .setHash(com.google.protobuf.ByteString.EMPTY)
                .build();
            storeMetadataAndIndex(id, metadata, duration, durationUnit);
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

    private void storeBlocksFromStream(ID id, InputStream stream, long duration, TimeUnit durationUnit) throws IOException {
        // We need to use a buffered stream here because we assume each read will fill the whole buffer.
        stream = new BufferedInputStream(stream);
        long blockNumber = 0;

        while (true) {
            byte[] bytesToStore = new byte[BLOCK_SIZE_IN_BYTES];
            int length = ByteStreams.read(stream, bytesToStore, 0, BLOCK_SIZE_IN_BYTES);
            // Store only relevant data if it only filled a partial block
            if (length == 0) {
                break;
            }
            Preconditions.checkNotNull(txnMgr);
            if (length < BLOCK_SIZE_IN_BYTES) {
                // This is the last block.
                storeBlockWithTransaction(id, blockNumber, PtBytes.head(bytesToStore, length), duration, durationUnit);
                break;
            } else {
                // Store a full block.
                storeBlockWithTransaction(id, blockNumber, bytesToStore, duration, durationUnit);
            }
            blockNumber++;
        }
    }

    protected void storeBlockWithTransaction(final ID id,
                                             final long blockNumber,
                                             final byte[] bytesToStore,
                                             final long duration,
                                             final TimeUnit durationUnit) {
        Preconditions.checkNotNull(txnMgr);
        txnMgr.runTaskThrowOnConflict(
                new TransactionTask<Void, RuntimeException>() {
                    @Override
                    public Void execute(Transaction t) throws RuntimeException {
                        storeBlock(t, id, blockNumber, bytesToStore, duration, durationUnit);
                        return null;
                    }
                });
    }

    protected abstract void storeBlock(Transaction t, ID id, long blockNumber, byte[] block, long duration, TimeUnit durationUnit);

    protected abstract void putMetadataAndHashIndexTask(Transaction t, Map<ID, StreamMetadata> streamIdsToMetadata, long duration, TimeUnit unit);

}

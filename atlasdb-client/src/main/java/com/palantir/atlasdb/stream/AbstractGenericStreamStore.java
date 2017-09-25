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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.CheckForNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.palantir.atlasdb.protos.generated.StreamPersistence.Status;
import com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.common.base.Throwables;
import com.palantir.util.ByteArrayIOStream;

public abstract class AbstractGenericStreamStore<T> implements GenericStreamStore<T> {
    protected static final Logger log = LoggerFactory.getLogger(AbstractGenericStreamStore.class);

    @CheckForNull protected final TransactionManager txnMgr;

    protected AbstractGenericStreamStore(TransactionManager txManager) {
        this.txnMgr = txManager;
    }

    private long getNumberOfBlocksFromMetadata(StreamMetadata metadata) {
        return (metadata.getLength() + BLOCK_SIZE_IN_BYTES - 1) / BLOCK_SIZE_IN_BYTES;
    }

    protected final StreamMetadata getEmptyMetadata() {
        return StreamMetadata.newBuilder()
            .setStatus(Status.STORING)
            .setLength(0L)
            .setHash(com.google.protobuf.ByteString.EMPTY)
            .build();
    }

    protected abstract long getInMemoryThreshold();

    @Override
    public InputStream loadStream(Transaction transaction, final T id) {
        StreamMetadata metadata = getMetadata(transaction, id);
        return getStream(transaction, id, metadata);
    }

    @Override
    public Optional<InputStream> loadSingleStream(Transaction transaction, final T id) {
        Map<T, StreamMetadata> idToMetadata = getMetadata(transaction, ImmutableSet.of(id));
        if (idToMetadata.isEmpty()) {
            return Optional.empty();
        }

        StreamMetadata metadata = getOnlyStreamMetadata(idToMetadata);
        return Optional.of(getStream(transaction, id, metadata));
    }

    @Override
    public Map<T, InputStream> loadStreams(Transaction transaction, Set<T> ids) {
        Map<T, StreamMetadata> idsToMetadata = getMetadata(transaction, ids);

        return idsToMetadata.entrySet().stream()
                .map(e -> Maps.immutableEntry(e.getKey(), getStream(transaction, e.getKey(), e.getValue())))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private InputStream getStream(Transaction transaction, T id, StreamMetadata metadata) {
        try {
            return tryGetStream(transaction, id, metadata);
        } catch (FileNotFoundException e) {
            log.error("Error opening temp file for stream {}", id, e);
            throw Throwables.rewrapAndThrowUncheckedException("Could not open temp file to create stream.", e);
        }
    }

    private InputStream tryGetStream(Transaction transaction, T id, StreamMetadata metadata)
            throws FileNotFoundException {
        checkStreamStored(id, metadata);
        if (metadata.getLength() == 0) {
            return new ByteArrayInputStream(new byte[0]);
        } else if (metadata.getLength() <= Math.min(getInMemoryThreshold(), BLOCK_SIZE_IN_BYTES)) {
            ByteArrayIOStream ios = new ByteArrayIOStream(Ints.saturatedCast(metadata.getLength()));
            loadSingleBlockToOutputStream(transaction, id, 0, ios);
            return ios.getInputStream();
        } else {
            return makeStream(transaction, id, metadata);
        }
    }

    private InputStream makeStream(Transaction parent, T id, StreamMetadata metadata) {
        long totalBlocks = getNumberOfBlocksFromMetadata(metadata);
        int blocksInMemory = getNumberOfBlocksThatFitInMemory();

        BlockGetter pageRefresher = new BlockGetter() {
            @Override
            public void get(long firstBlock, long numBlocks, OutputStream destination) {
                if (parent.isUncommitted()) {
                    loadNBlocksToOutputStream(parent, id, firstBlock, numBlocks, destination);
                } else {
                    txnMgr.runTaskReadOnly(txn -> {
                        loadNBlocksToOutputStream(txn, id, firstBlock, numBlocks, destination);
                        return null;
                    });
                }
            }

            @Override
            public int expectedBlockLength() {
                return BLOCK_SIZE_IN_BYTES;
            }
        };

        try {
            return BlockConsumingInputStream.create(pageRefresher, totalBlocks, blocksInMemory);
        } catch (IOException e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    protected int getNumberOfBlocksThatFitInMemory() {
        int inMemoryThreshold = (int) getInMemoryThreshold(); // safe; actually defined as an int in generated code.
        int blocksInMemory = inMemoryThreshold / BLOCK_SIZE_IN_BYTES;
        return Math.max(1, blocksInMemory);
    }

    @Override
    public final File loadStreamAsFile(Transaction transaction, T id) {
        StreamMetadata metadata = getMetadata(transaction, id);
        checkStreamStored(id, metadata);
        return loadToNewTempFile(transaction, id, metadata);
    }

    private File loadToNewTempFile(Transaction transaction, T id, StreamMetadata metadata) {
        try {
            File file = createTempFile(id);
            writeStreamToFile(transaction, id, metadata, file);
            return file;
        } catch (IOException e) {
            log.error("Could not create temp file for stream id {}", id, e);
            throw Throwables.rewrapAndThrowUncheckedException("Could not create file to create stream.", e);
        }
    }

    private void checkStreamStored(T id, StreamMetadata metadata) {
        if (metadata == null) {
            log.error("Error loading stream {} because it was never stored.", id);
            throw new IllegalArgumentException("Unable to load stream " + id + " because it was never stored.");
        } else if (metadata.getStatus() != Status.STORED) {
            log.error("Error loading stream {} because it has status {}", id, metadata.getStatus());
            throw new IllegalArgumentException("Could not get stream because it was not fully stored.");
        }
    }

    private void writeStreamToFile(Transaction transaction, T id, StreamMetadata metadata, File file)
            throws FileNotFoundException {
        FileOutputStream fos = new FileOutputStream(file);
        try {
            tryWriteStreamToFile(transaction, id, metadata, fos);
        } catch (IOException e) {
            log.error("Could not finish streaming blocks to file for stream {}", id, e);
            throw Throwables.rewrapAndThrowUncheckedException("Error writing blocks while opening a stream.", e);
        } finally {
            try {
                fos.close();
            } catch (IOException e) {
                // Do nothing
            }
        }
    }

    private void loadNBlocksToOutputStream(
            Transaction tx,
            T streamId,
            long firstBlock,
            long numBlocks,
            OutputStream os) {
        for (long i = 0; i < numBlocks; i++) {
            loadSingleBlockToOutputStream(tx, streamId, firstBlock + i, os);
        }
    }

    // This method is overridden in generated code. Changes to this method may have unintended consequences.
    protected void tryWriteStreamToFile(Transaction transaction, T id, StreamMetadata metadata, FileOutputStream fos)
            throws IOException {
        long numBlocks = getNumberOfBlocksFromMetadata(metadata);
        for (long i = 0; i < numBlocks; i++) {
            loadSingleBlockToOutputStream(transaction, id, i, fos);
        }
        fos.close();
    }

    protected abstract File createTempFile(T id) throws IOException;

    protected abstract void loadSingleBlockToOutputStream(Transaction tx, T streamId, long blockId, OutputStream os);

    protected abstract Map<T, StreamMetadata> getMetadata(Transaction tx, Set<T> streamIds);

    private StreamMetadata getMetadata(Transaction transaction, T id) {
        return getOnlyStreamMetadata(getMetadata(transaction, ImmutableSet.of(id)));
    }

    private StreamMetadata getOnlyStreamMetadata(Map<T, StreamMetadata> idToMetadata) {
        return Iterables.getOnlyElement(idToMetadata.values());
    }
}

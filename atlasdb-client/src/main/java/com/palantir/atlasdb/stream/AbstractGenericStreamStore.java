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

import javax.annotation.CheckForNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.palantir.atlasdb.protos.generated.StreamPersistence.Status;
import com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.common.base.Throwables;
import com.palantir.util.ByteArrayIOStream;

public abstract class AbstractGenericStreamStore<ID> implements GenericStreamStore<ID> {
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
    public InputStream loadStream(Transaction transaction, final ID id) {
        StreamMetadata metadata = getMetadata(transaction, id);
        return getStream(transaction, id, metadata);
    }

    @Override
    public final Optional<InputStream> loadSingleStream(Transaction transaction, final ID id) {
        Map<ID, StreamMetadata> idToMetadata = getMetadata(transaction, Sets.newHashSet(id));
        if (idToMetadata.isEmpty()) {
            return Optional.empty();
        }

        StreamMetadata metadata = Iterables.getOnlyElement(idToMetadata.values());
        return Optional.of(getStream(transaction, id, metadata));
    }

    @Override
    public final Map<ID, InputStream> loadStreams(Transaction transaction, Set<ID> ids) {
        Map<ID, InputStream> ret = Maps.newHashMap();
        Map<ID, StreamMetadata> idsToMetadata = getMetadata(transaction, ids);
        for (Map.Entry<ID, StreamMetadata> entry : idsToMetadata.entrySet()) {
            ID id = entry.getKey();
            ret.put(id, getStream(transaction, id, entry.getValue()));
        }
        return ret;
    }

    private InputStream getStream(Transaction transaction, ID id, StreamMetadata metadata) {
        try {
            return tryGetStream(transaction, id, metadata);
        } catch (FileNotFoundException e) {
            log.error("Error opening temp file for stream {}", id, e);
            throw Throwables.rewrapAndThrowUncheckedException("Could not open temp file to create stream.", e);
        }
    }

    private InputStream tryGetStream(Transaction transaction, ID id, StreamMetadata metadata)
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

    private InputStream makeStream(Transaction parent, ID id, StreamMetadata metadata) {
        long totalBlocks = getNumberOfBlocksFromMetadata(metadata);
        int blocksInMemory = getNumberOfBlocksThatFitInMemory();

        BlockGetter pageRefresher = new BlockGetter() {
            @Override
            public void get(long firstBlock, long numBlocks, OutputStream destination) {
                Preconditions.checkState(!parent.isUncommitted(),
                        "Tried to read from a stream while the transaction used to fetch the stream was still open. "
                                + "This is forbidden, because it would cause a transaction to run inside another one.");
                txnMgr.runTaskReadOnly(txn -> {
                    for (long i = 0; i < numBlocks; i++) {
                        loadSingleBlockToOutputStream(txn, id, firstBlock + i, destination);
                    }
                    return null;
                });
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
    public final File loadStreamAsFile(Transaction transaction, ID id) {
        StreamMetadata metadata = getMetadata(transaction, id);
        checkStreamStored(id, metadata);
        return loadToNewTempFile(transaction, id, metadata);
    }

    private File loadToNewTempFile(Transaction transaction, ID id, StreamMetadata metadata) {
        try {
            File file = createTempFile(id);
            writeStreamToFile(transaction, id, metadata, file);
            return file;
        } catch (IOException e) {
            log.error("Could not create temp file for stream id {}", id, e);
            throw Throwables.rewrapAndThrowUncheckedException("Could not create file to create stream.", e);
        }
    }

    private void checkStreamStored(ID id, StreamMetadata metadata) {
        if (metadata == null) {
            log.error("Error loading stream {} because it was never stored.", id);
            throw new IllegalArgumentException("Unable to load stream " + id + " because it was never stored.");
        } else if (metadata.getStatus() != Status.STORED) {
            log.error("Error loading stream {} because it has status {}", id, metadata.getStatus());
            throw new IllegalArgumentException("Could not get stream because it was not fully stored.");
        }
    }

    private void writeStreamToFile(Transaction transaction, ID id, StreamMetadata metadata, File file)
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

    // This method is overridden in generated code. Changes to this method may have unintended consequences.
    protected void tryWriteStreamToFile(Transaction transaction, ID id, StreamMetadata metadata, FileOutputStream fos)
            throws IOException {
        long numBlocks = getNumberOfBlocksFromMetadata(metadata);
        for (long i = 0; i < numBlocks; i++) {
            loadSingleBlockToOutputStream(transaction, id, i, fos);
        }
        fos.close();
    }

    protected abstract File createTempFile(ID id) throws IOException;

    protected abstract void loadSingleBlockToOutputStream(Transaction tx, ID streamId, long blockId, OutputStream os);

    protected abstract Map<ID, StreamMetadata> getMetadata(Transaction tx, Set<ID> streamIds);

    private StreamMetadata getMetadata(Transaction transaction, ID id) {
        return Iterables.getOnlyElement(getMetadata(transaction, Sets.newHashSet(id)).entrySet()).getValue();
    }
}

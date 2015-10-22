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
import java.util.Set;

import javax.annotation.CheckForNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import com.palantir.util.file.DeleteOnCloseFileInputStream;

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
    public final InputStream loadStream(Transaction t, final ID id) {
        try {
            StreamMetadata metadata = checkStreamStored(id, getMetadata(t, id));
            if (metadata.getLength() == 0) {
                return new ByteArrayInputStream(new byte[0]);
            } else if (metadata.getLength() <= Math.min(getInMemoryThreshold(), BLOCK_SIZE_IN_BYTES)) {
                ByteArrayIOStream ios = new ByteArrayIOStream(Ints.saturatedCast(metadata.getLength()));
                loadSingleBlockToOutputStream(t, id, 0, ios);
                return ios.getInputStream();
            } else {
                File file = loadToNewTempFile(t, id, metadata);
                return new DeleteOnCloseFileInputStream(file);
            }
        } catch (FileNotFoundException e) {
            log.error("Error opening temp file for stream " + id, e);
            throw Throwables.rewrapAndThrowUncheckedException("Could not open temp file to create stream.", e);
        }
    }

    @Override
    public final Map<ID, InputStream> loadStreams(Transaction t, Set<ID> ids) {
        Map<ID, InputStream> ret = Maps.newHashMap();
        Map<ID, StreamMetadata> idsToMetadata = getMetadata(t, ids);
        for (Map.Entry<ID, StreamMetadata> entry : idsToMetadata.entrySet()) {
            ID id = entry.getKey();
            try {
                StreamMetadata metadata = checkStreamStored(id, entry.getValue());
                if (metadata.getLength() == 0) {
                    ret.put(id, new ByteArrayInputStream(new byte[0]));
                } else if (metadata.getLength() <= Math.min(getInMemoryThreshold(), BLOCK_SIZE_IN_BYTES)) {
                    ByteArrayIOStream ios = new ByteArrayIOStream(Ints.saturatedCast(metadata.getLength()));
                    loadSingleBlockToOutputStream(t, id, 0, ios);
                    ret.put(id, ios.getInputStream());
                } else {
                    File file = loadToNewTempFile(t, id, metadata);
                    ret.put(id, new DeleteOnCloseFileInputStream(file));
                }
            } catch (FileNotFoundException e) {
                log.error("Error opening temp file for stream " + id, e);
                throw Throwables.rewrapAndThrowUncheckedException("Could not open temp file to create stream.", e);
            }
        }
        return ret;
    }

    @Override
    public final File loadStreamAsFile(Transaction t, ID id) {
        StreamMetadata metadata = checkStreamStored(id, getMetadata(t, id));
        return loadToNewTempFile(t, id, metadata);
    }

    private File loadToNewTempFile(Transaction t, ID id, StreamMetadata metadata) {
        try {
            File file = createTempFile(id);
            writeStreamToFile(t, id, metadata, file);
            return file;
        } catch (IOException e) {
            log.error("Could not create temp file for stream id " + id, e);
            throw Throwables.rewrapAndThrowUncheckedException("Could not create file to create stream.", e);
        }
    }

    private StreamMetadata checkStreamStored(ID id, StreamMetadata metadata) {
        if (metadata == null) {
            log.error("Error loading stream " + id + " because it was never stored.");
            throw new IllegalArgumentException("Unable to load stream " + id + " because it was never stored.");
        } else if (metadata.getStatus() != Status.STORED) {
            log.error("Error loading stream " + id + " because it has status " + metadata.getStatus());
            throw new IllegalArgumentException("Could not get stream because it was not fully stored.");
        }
        return metadata;
    }

    private void writeStreamToFile(Transaction t, ID id, StreamMetadata metadata, File file) throws FileNotFoundException {
        FileOutputStream fos = new FileOutputStream(file);
        try {
            tryWriteStreamToFile(t, id, metadata, fos);
        } catch (IOException e) {
            log.error("Could not finish streaming blocks to file for stream " + id, e);
            throw Throwables.rewrapAndThrowUncheckedException("Error writing blocks while opening a stream.", e);
        } finally {
            try { fos.close(); } catch (IOException e) {}
        }
    }

    private void tryWriteStreamToFile(Transaction t, ID id, StreamMetadata metadata, FileOutputStream fos) throws IOException {
        long numBlocks = getNumberOfBlocksFromMetadata(metadata);
        for (long i = 0; i < numBlocks; i++) {
            loadSingleBlockToOutputStream(t, id, i, fos);
        }
        fos.close();
    }

    private StreamMetadata getMetadata(Transaction t, ID id) {
        return Iterables.getOnlyElement(getMetadata(t, Sets.newHashSet(id)).entrySet()).getValue();
    }

    protected abstract File createTempFile(ID id) throws IOException;

    protected abstract void loadSingleBlockToOutputStream(Transaction t, ID streamId, long blockId, OutputStream os);

    protected abstract Map<ID, StreamMetadata> getMetadata(Transaction t, Set<ID> streamIds);
}

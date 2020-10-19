/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.stream;

import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.util.Pair;
import com.palantir.util.crypto.Sha256Hash;
import java.io.InputStream;
import java.util.Map;

/**
 * Interface for storing streams specifically for atlasdb.
 */
public interface PersistentStreamStore extends GenericStreamStore<Long> {

    /**
     * This method will return a valid stream id.  If this stream already exists in the store
     * with the same hash that id will be returned so we don't store multiple copies.
     * <p>
     * This will mark the stream as being used by the <code>reference</code>.  Marking the streams
     * does reference counting.  Any streams that are unreferenced will be garbage collected.
     * <p>
     * When the stream id is returned, it should be stored in some cell with the reference
     * so that when the reference is deleted, {@link #unmarkStreamAsUsed(Transaction, long, byte[])} can be called.
     * <p>
     * This takes a transaction and marks this stream as used once it is stored. The downside is that
     * This transaction must be open for the whole time it takes to store this stream so it can't be used
     * with really large streams cause it will take too long.
     *
     * @return stream id
     */
    long getByHashOrStoreStreamAndMarkAsUsed(Transaction tx, Sha256Hash hash, InputStream stream, byte[] reference);

    void markStreamAsUsed(Transaction tx, long streamId, byte[] reference) throws StreamCleanedException;

    void markStreamsAsUsed(Transaction tx, Map<Long, byte[]> streamIdsToReference) throws StreamCleanedException;

    /**
     * This removes the index references from streamId -&gt; reference and deletes streams with no remaining references.
     */
    void unmarkStreamAsUsed(Transaction tx, long streamId, byte[] reference);

    void unmarkStreamsAsUsed(Transaction tx, Map<Long, byte[]> streamIdsToReference);

    /**
     * This method will store a stream, but it will not have any references.  This means that if cleanup
     * runs for this stream it will be deleted.  The cleanup time is configured in
     * <code>Cleaner.getTransactionReadTimeoutMillis()</code> and defaults to 24 hours.
     * <p>
     * Usually after this store is complete a transaction will run and use the streamId and call
     * {@link #markStreamAsUsed(Transaction, long, byte[])}.
     * <p>
     * This is the best method for very large streams because they shouldn't be stored inside a transaction
     * like {@link #getByHashOrStoreStreamAndMarkAsUsed(Transaction, Sha256Hash, InputStream, byte[])} because
     * it will cause that transaction to be too long running.
     * <p>
     * Users of this method should be ready to retry if their stream was cleaned up between storing and marking.
     * {@link #getByHashOrStoreStreamAndMarkAsUsed(Transaction, Sha256Hash, InputStream, byte[])} is much prefered
     * because of this complexity assuming the stream isn't too large (100MB or less).
     */
    Pair<Long, Sha256Hash> storeStream(InputStream stream);

    /**
     * This method will store a stream, but it will not have any references.  Unlike with
     * {@link #storeStream(InputStream)} a transaction is required.
     * <p>
     * Use the stream ids returned here and the same transaction to also {@link #markStreamsAsUsed(Transaction, Map)}
     * to ensure that the streams are not cleaned-up.
     * <p>
     * Although this method provides performance gains by batching the relevant underlying code it can also also
     * cause the underlying transaction to be long-running.  When possible, individually add streams using
     * {@link #getByHashOrStoreStreamAndMarkAsUsed(Transaction, Sha256Hash, InputStream, byte[])} or
     * {@link #storeStream(InputStream)}.
     */
    Map<Long, Sha256Hash> storeStreams(Transaction tx, Map<Long, InputStream> streams);
}

/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.v2.api.transaction.scanner;

import java.util.function.Function;

import com.palantir.atlasdb.v2.api.iterators.AsyncIterators;
import com.palantir.atlasdb.v2.api.api.NewValue;
import com.palantir.atlasdb.v2.api.api.Kvs;
import com.palantir.atlasdb.v2.api.api.NewLocks;
import com.palantir.atlasdb.v2.api.api.Timestamps;
import com.palantir.atlasdb.v2.api.transaction.scanner.ReadReportingReader.RecordingNewValue;

public final class ReaderFactory {
    private final AsyncIterators iterators;
    private final Kvs kvs;
    private final NewLocks locks;
    private final Timestamps timestamps;

    public ReaderFactory(
            AsyncIterators iterators,
            Kvs kvs,
            NewLocks locks,
            Timestamps timestamps) {
        this.iterators = iterators;
        this.kvs = kvs;
        this.locks = locks;
        this.timestamps = timestamps;
    }

    public Kvs kvs() {
        return kvs;
    }

    public <T extends NewValue, R extends Reader<T>> Function<R, Reader<T>> readAtCommitTimestamp() {
        return ReadAtCommitTimestamp::new;
    }

    public <T extends NewValue, R extends Reader<T>> Function<R, Reader<NewValue>> mergeInTransactionWrites() {
        return reader -> new MergeInTransactionWritesReader(iterators, reader);
    }

    public <T extends NewValue> Function<Reader<T>, CheckImmutableLocksReader<T>> checkImmutableLocks() {
        return reader -> new CheckImmutableLocksReader<T>(reader, iterators, locks);
    }

    public <K extends Kvs> Function<K, ReadCommittedDataReader> readCommittedData() {
        return reader -> new ReadCommittedDataReader(reader, iterators, locks);
    }

    public <T extends NewValue, R extends Reader<T>> Function<R, Reader<RecordingNewValue>> reportReads() {
        return reader -> new ReadReportingReader<>(reader, iterators);
    }

    public <T extends NewValue, R extends Reader<T>> Function<R, Reader<T>> stopAfterMarker() {
        return reader -> new StopAfterMarkerReader<>(iterators, reader);
    }

    public <T extends NewValue, R extends Reader<T>> Function<R, Reader<T>> orderValidating() {
        return OrderValidatingReader::new;
    }
}

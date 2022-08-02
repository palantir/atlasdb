/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.atomic;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.transaction.impl.TransactionStatusUtils;
import com.palantir.atlasdb.transaction.service.TransactionStatus;
import com.palantir.common.streams.KeyedStream;
import java.util.Map;
import java.util.Optional;

public class TimestampExtractingAtomicTable implements AtomicTable<Long, Long> {
    private final AtomicTable<Long, TransactionStatus> delegate;

    public TimestampExtractingAtomicTable(AtomicTable<Long, TransactionStatus> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void updateMultiple(Map<Long, Long> keyValues) throws KeyAlreadyExistsException {
        delegate.updateMultiple(KeyedStream.stream(keyValues)
                .map(TransactionStatusUtils::fromTimestamp)
                .collectToMap());
    }

    @Override
    public ListenableFuture<Map<Long, Long>> get(Iterable<Long> keys) {
        return Futures.transform(
                delegate.get(keys),
                statuses -> KeyedStream.stream(statuses)
                        .map(TransactionStatusUtils::maybeGetCommitTs)
                        .flatMap(Optional::stream)
                        .collectToMap(),
                MoreExecutors.directExecutor());
    }
}

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
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.Map;
import java.util.Optional;

/**
 * A layer on top of {@code AtomicTable<Long, TransactionStatus>} that transforms a TransactionStatus to {@code Long}
 * if the transaction is committed/aborted.
 * This layer is only meant to be used for transactions on schema version < 4.
 * */
public class TimestampExtractingAtomicTable implements AtomicTable<Long, Long> {
    private final AtomicTable<Long, TransactionStatus> delegate;

    public TimestampExtractingAtomicTable(AtomicTable<Long, TransactionStatus> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void markInProgress(Iterable<Long> keys) {
        delegate.markInProgress(keys);
    }

    @Override
    public void updateMultiple(Map<Long, Long> keyValues) throws KeyAlreadyExistsException {
        delegate.updateMultiple(KeyedStream.stream(keyValues)
                .map(TransactionStatusUtils::fromTimestamp)
                .collectToMap());
    }

    /**
     * Returns a map of commit timestamp against the list of start timestamps supplied as arg.
     * For transactions that are successfully committed, returns the respective commit timestamps.
     * For transactions that are aborted, returns -1.
     * Start timestamps for transactions that are neither committed nor aborted are not included in the result.
     * */
    @Override
    public ListenableFuture<Map<Long, Long>> get(Iterable<Long> keys) {
        return Futures.transform(
                delegate.get(keys),
                statuses -> {
                    if (statuses.values().stream().anyMatch(TransactionStatus.unknown()::equals)) {
                        throw new SafeIllegalStateException("There has been a mistake in the wiring as "
                                + "transactions that do not support transaction table sweep should not be seeing "
                                + "`unknown` transaction status.");
                    }
                    return KeyedStream.stream(statuses)
                            .map(TransactionStatusUtils::maybeGetCommitTs)
                            .flatMap(Optional::stream)
                            .collectToMap();
                },
                MoreExecutors.directExecutor());
    }
}

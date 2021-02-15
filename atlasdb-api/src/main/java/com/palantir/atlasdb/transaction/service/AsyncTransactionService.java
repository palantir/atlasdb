/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.service;

import com.google.common.util.concurrent.ListenableFuture;
import java.util.Map;

public interface AsyncTransactionService {
    /**
     * Gets the commit timestamp associated with a given {@code startTimestamp} in a future, potentially computing
     * it asynchronously. Non-null future result responses may be cached on the client-side. Null responses must not be
     * cached, as they could subsequently be updated.
     *
     * Future result may return null, which means that the transaction in question had not been committed, at
     * least at some point between the request being made and it returning.
     *
     * @param startTimestamp start timestamp of the transaction being looked up
     * @return {@link ListenableFuture} containing the timestamp which the transaction committed at, or null if the
     * transaction had not committed yet
     */
    ListenableFuture<Long> getAsync(long startTimestamp);

    /**
     * Gets the commit timestamps associated with start timestamps given in {@code startTimestamps}, potentially
     * computing them asynchronously. Returned entries may be cached on the client-side. Entries which are missing which
     * are equivalent to null responses in {@link AsyncTransactionService#getAsync(long)} must not be cached, as they
     * could subsequently be updated.
     *
     * Future result is never null. However, missing key-value pairs mean that transactions in question have not been
     * committed, at least at some point between the request being made and it returning.
     *
     * @param startTimestamps start timestamps of the transactions being looked up
     * @return {@link ListenableFuture} containing the map from a transaction start timestamp to transaction commit
     * timestamp, possibly missing entries if relevant transactions have not committed yet
     */
    ListenableFuture<Map<Long, Long>> getAsync(Iterable<Long> startTimestamps);
}

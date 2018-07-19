/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.service;

import java.util.List;
import java.util.Map;
import java.util.function.LongPredicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;

/**
 * A DynamicSplittingTransactionService delegates between two {@link TransactionService}s, depending on whether the
 * start timestamp for a given transaction satisfies the {@link LongPredicate} provided.
 *
 * Note: We require that once the {@link LongPredicate} has been invoked on a given timestamp, it must return the
 * same value for all future invocations on that timestamp. The behaviour of the {@link LongPredicate} on values that
 * this {@link TransactionService} has not been queried for is allowed to change over time.
 */
public class DynamicSplittingTransactionService implements TransactionService {
    private final LongPredicate shouldUseFirstService;
    private final TransactionService firstService;
    private final TransactionService secondService;

    public DynamicSplittingTransactionService(
            LongPredicate shouldUseFirstService,
            TransactionService firstService,
            TransactionService secondService) {
        this.shouldUseFirstService = shouldUseFirstService;
        this.firstService = firstService;
        this.secondService = secondService;
    }

    @Override
    public Long get(long startTimestamp) {
        TransactionService targetService = getTargetServiceForStartTimestamp(startTimestamp);
        return targetService.get(startTimestamp);
    }

    @Override
    public Map<Long, Long> get(Iterable<Long> startTimestamps) {
        // TODO (jkong): If this is slow, rewrite without streams
        Map<Boolean, List<Long>> queries = StreamSupport.stream(startTimestamps.spliterator(), false)
                .collect(Collectors.groupingBy(shouldUseFirstService::test));

        return ImmutableMap.<Long, Long>builder()
                .putAll(invokeGetIfNonNull(firstService, queries.get(true)))
                .putAll(invokeGetIfNonNull(secondService, queries.get(false)))
                .build();
    }

    @Override
    public void putUnlessExists(long startTimestamp, long commitTimestamp) throws KeyAlreadyExistsException {
        TransactionService targetService = getTargetServiceForStartTimestamp(startTimestamp);
        targetService.putUnlessExists(startTimestamp, commitTimestamp);
    }

    private Map<Long, Long> invokeGetIfNonNull(TransactionService service, List<Long> startTimestamps) {
        if (startTimestamps == null) {
            return ImmutableMap.of();
        }
        return service.get(startTimestamps);
    }

    private TransactionService getTargetServiceForStartTimestamp(long startTimestamp) {
        return shouldUseFirstService.test(startTimestamp) ? firstService : secondService;
    }
}

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
import java.util.function.LongSupplier;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;

public class TableSplittingTransactionService implements TransactionService {
    private final TransactionService firstService;
    private final TransactionService secondService;
    private final LongSupplier splitPointSupplier;

    public TableSplittingTransactionService(
            TransactionService firstService,
            TransactionService secondService,
            LongSupplier splitPointSupplier) {
        this.firstService = firstService;
        this.secondService = secondService;
        this.splitPointSupplier = splitPointSupplier;
    }

    @Override
    public Long get(long startTimestamp) {
        if (startTimestamp < splitPointSupplier.getAsLong()) {
            return firstService.get(startTimestamp);
        }
        return secondService.get(startTimestamp);
    }

    @Override
    public Map<Long, Long> get(Iterable<Long> startTimestamps) {
        long splitPoint = splitPointSupplier.getAsLong();

        List<Long> firstTimestamps = Lists.newArrayList();
        List<Long> secondTimestamps = Lists.newArrayList();

        for (long timestamp : startTimestamps) {
            if (timestamp < splitPoint) {
                firstTimestamps.add(timestamp);
            } else {
                secondTimestamps.add(timestamp);
            }
        }

        Map<Long, Long> firstTimestampValues = firstService.get(firstTimestamps);
        Map<Long, Long> secondTimestampValues = secondService.get(secondTimestamps);

        return ImmutableMap
                .<Long, Long>builderWithExpectedSize(firstTimestampValues.size() + secondTimestampValues.size())
                .putAll(firstTimestampValues)
                .putAll(secondTimestampValues)
                .build();
    }

    @Override
    public void putUnlessExists(long startTimestamp, long commitTimestamp) throws KeyAlreadyExistsException {
        if (startTimestamp < splitPointSupplier.getAsLong()) {
            firstService.putUnlessExists(startTimestamp, commitTimestamp);
        } else {
            secondService.putUnlessExists(startTimestamp, commitTimestamp);
        }
    }
}

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

package com.palantir.atlasdb.debug;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.common.persist.Persistable;
import com.palantir.common.streams.KeyedStream;

public class WritesDigestEmitter {

    private final TransactionService transactionService;
    private final TableReference tableReference;
    private final KeyValueService keyValueService;

    public WritesDigestEmitter(TransactionManager transactionManager, TableReference tableReference) {
        this.keyValueService = transactionManager.getKeyValueService();
        this.transactionService = transactionManager.getTransactionService();
        this.tableReference = tableReference;
    }

    public <T> WritesDigest<T> getDigest(Persistable row, byte[] columnName, Function<Value, T> deserializer) {
        Cell asCell = Cell.create(row.persistToBytes(), columnName);

        Multimap<Cell, Long> allWrittenCells =
                keyValueService.getAllTimestamps(tableReference, ImmutableSet.of(asCell), Long.MAX_VALUE);

        Collection<Long> allWrittenTimestamps = allWrittenCells.get(asCell);

        Map<Long, Long> transactionCommitStatuses = transactionService.get(allWrittenTimestamps);
        Map<Long, Long> completedOrAbortedTransactions = KeyedStream.stream(transactionCommitStatuses)
                .filter(Objects::nonNull)
                .collectToMap();

        Set<Long> inProgressTransactions = Sets.difference(
                transactionCommitStatuses.keySet(),
                completedOrAbortedTransactions.keySet());

        Set<Long> boundsForCell = allWrittenTimestamps.stream()
                .map(existingTimestamp -> existingTimestamp + 1)
                .collect(Collectors.toSet());

        Map<Long, T> allSeenWrittenValues = boundsForCell.stream()
                .map(bound -> ImmutableMap.of(asCell, bound))
                .map(request -> keyValueService.get(tableReference, request))
                .map(result -> result.get(asCell))
                .filter(Objects::nonNull)
                .collect(KeyedStream.toKeyedStream())
                .mapKeys(Value::getTimestamp)
                .map(deserializer)
                .collectToMap();

        return ImmutableWritesDigest.<T>builder()
                .allWrittenTimestamps(allWrittenTimestamps)
                .completedOrAbortedTransactions(completedOrAbortedTransactions)
                .inProgressTransactions(inProgressTransactions)
                .allWrittenValuesDeserialized(allSeenWrittenValues)
                .build();
    }

}

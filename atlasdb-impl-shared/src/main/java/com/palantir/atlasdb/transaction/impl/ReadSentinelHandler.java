/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl;

import com.google.common.collect.Maps;
import com.palantir.atlasdb.cell.api.TransactionKeyValueService;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.transaction.api.OrphanedSentinelDeleter;
import com.palantir.atlasdb.transaction.api.Transaction.TransactionType;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public final class ReadSentinelHandler {
    private final TransactionService transactionService;
    private final TransactionReadSentinelBehavior readSentinelBehavior;
    private final OrphanedSentinelDeleter orphanedSentinelDeleter;

    public ReadSentinelHandler(
            TransactionService transactionService,
            TransactionReadSentinelBehavior readSentinelBehavior,
            OrphanedSentinelDeleter orphanedSentinelDeleter) {
        this.transactionService = transactionService;
        this.readSentinelBehavior = readSentinelBehavior;
        this.orphanedSentinelDeleter = orphanedSentinelDeleter;
    }

    /**
     * A sentinel becomes orphaned if the table has been truncated between the time where the write occurred and where
     * it was truncated. In this case, there is a chance that we end up with a sentinel with no valid AtlasDB cell
     * covering it. In this case, we ignore it.
     */
    public Set<Cell> findAndMarkOrphanedSweepSentinelsForDeletion(
            TransactionKeyValueService transactionKeyValueService, TableReference table, Map<Cell, Value> rawResults) {
        Set<Cell> sweepSentinels = Maps.filterValues(rawResults, ReadSentinelHandler::isSweepSentinel)
                .keySet();
        if (sweepSentinels.isEmpty()) {
            return sweepSentinels;
        }

        // for each sentinel, start at long max. Then iterate down with each found uncommitted value.
        // if committed value seen, stop: the sentinel is not orphaned
        // if we get back -1, the sentinel is orphaned
        Map<Cell, Long> timestampCandidates = new HashMap<>(
                transactionKeyValueService.getLatestTimestamps(table, Maps.asMap(sweepSentinels, x -> Long.MAX_VALUE)));
        Set<Cell> actualOrphanedSentinels = new HashSet<>();

        while (!timestampCandidates.isEmpty()) {
            Map<SentinelType, Map<Cell, Long>> sentinelTypeToTimestamps = timestampCandidates.entrySet().stream()
                    .collect(Collectors.groupingBy(
                            entry -> entry.getValue() == Value.INVALID_VALUE_TIMESTAMP
                                    ? SentinelType.DEFINITE_ORPHANED
                                    : SentinelType.INDETERMINATE,
                            Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

            Map<Cell, Long> definiteOrphans = sentinelTypeToTimestamps.get(SentinelType.DEFINITE_ORPHANED);
            if (definiteOrphans != null) {
                actualOrphanedSentinels.addAll(definiteOrphans.keySet());
            }

            Map<Cell, Long> cellsToQuery = sentinelTypeToTimestamps.get(SentinelType.INDETERMINATE);
            if (cellsToQuery == null) {
                break;
            }
            Set<Long> committedStartTimestamps = KeyedStream.stream(transactionService.get(cellsToQuery.values()))
                    .filter(Objects::nonNull)
                    .keys()
                    .collect(Collectors.toSet());

            Map<Cell, Long> nextTimestampCandidates = KeyedStream.stream(cellsToQuery)
                    .filter(cellStartTimestamp -> !committedStartTimestamps.contains(cellStartTimestamp))
                    .collectToMap();
            timestampCandidates = transactionKeyValueService.getLatestTimestamps(table, nextTimestampCandidates);
        }

        orphanedSentinelDeleter.scheduleSentinelsForDeletion(table, actualOrphanedSentinels);
        return actualOrphanedSentinels;
    }

    public void handleReadSentinel() {
        // This means that this transaction started too long ago. When we do garbage collection,
        // we clean up old values, and this transaction started at a timestamp before the garbage collection.
        switch (readSentinelBehavior) {
            case IGNORE:
                break;
            case THROW_EXCEPTION:
                throw new TransactionFailedRetriableException("Tried to read a value that has been "
                        + "deleted. This can be caused by hard delete transactions using the type "
                        + TransactionType.AGGRESSIVE_HARD_DELETE
                        + ". It can also be caused by transactions taking too long, or"
                        + " its locks expired. Retrying it should work.");
            default:
                throw new SafeIllegalStateException(
                        "Invalid read sentinel behavior", SafeArg.of("readSentinelBehaviour", readSentinelBehavior));
        }
    }

    private static boolean isSweepSentinel(Value value) {
        return value.getTimestamp() == Value.INVALID_VALUE_TIMESTAMP;
    }

    private enum SentinelType {
        DEFINITE_ORPHANED,
        INDETERMINATE;
    }
}

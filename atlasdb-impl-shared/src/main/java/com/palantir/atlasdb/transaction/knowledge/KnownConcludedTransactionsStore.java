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

package com.palantir.atlasdb.transaction.knowledge;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.common.concurrent.CoalescingSupplier;
import com.palantir.conjure.java.serialization.ObjectMappers;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Stores information about a {@link TimestampRangeSet} in a single cell in a single table in an underlying
 * key-value-service.
 */
@ThreadSafe
public final class KnownConcludedTransactionsStore {
    private static final SafeLogger log = SafeLoggerFactory.get(KnownConcludedTransactionsStore.class);
    private static final ObjectMapper OBJECT_MAPPER = ObjectMappers.newSmileServerObjectMapper();
    private static final Cell DEFAULT_CELL = Cell.create(PtBytes.toBytes("r"), PtBytes.toBytes("c"));
    private static final int MAX_ATTEMPTS = 20;

    private final KeyValueService keyValueService;
    private final TableReference tableReference;
    private final Cell valueCell;
    private final CoalescingSupplier<Optional<ReadResult>> valueReader;

    private KnownConcludedTransactionsStore(
            KeyValueService keyValueService, TableReference tableReference, Cell valueCell) {
        this.keyValueService = keyValueService;
        this.tableReference = tableReference;
        this.valueCell = valueCell;
        this.valueReader = new CoalescingSupplier<>(this::getInternal);
    }

    public static KnownConcludedTransactionsStore create(KeyValueService keyValueService) {
        return new KnownConcludedTransactionsStore(
                keyValueService, TransactionConstants.KNOWN_CONCLUDED_TRANSACTIONS_TABLE, DEFAULT_CELL);
    }

    public Optional<TimestampRangeSet> get() {
        return valueReader.get().map(ReadResult::timestampRangeSet);
    }

    /**
     * If this method completes non-exceptionally, it is guaranteed that the {@link TimestampRangeSet} persisted in
     * the database contains the provided {@code timestampRangeToAdd}.
     *
     * In the event of multiple concurrent calls to this method, it is guaranteed that if they all resolve, then all
     * of the {@code timestampRangeToAdd} arguments will be enclosed in the final state of the
     * {@link TimestampRangeSet} that has been persisted in the database.
     */
    public void supplement(Range<Long> timestampRangeToAdd) {
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
            Optional<ReadResult> readResult = getInternal();

            if (isRangeContained(readResult, timestampRangeToAdd)) {
                return;
            }

            CheckAndSetRequest checkAndSetRequest = getCheckAndSetRequest(readResult, timestampRangeToAdd);
            try {
                keyValueService.checkAndSet(checkAndSetRequest);
                return;
            } catch (CheckAndSetException checkAndSetException) {
                // swallow for retrying
                log.info(
                        "Attempt to update the KVS for the timestamp range store failed, possibly because someone "
                                + "else wrote to this table concurrently.",
                        checkAndSetException);
            }
        }
        log.warn("Unable to supplement the set of concluded timestamps with a new timestamp range. This may be "
                + "because the database is momentarily unavailable, or because of particularly high contention.");
        throw new SafeIllegalStateException("Unable to supplement set of concluded timestamps.");
    }

    private boolean isRangeContained(Optional<ReadResult> containingRanges, Range<Long> timestampRangeToTest) {
        return containingRanges.map(ReadResult::timestampRangeSet)
                .map(ranges -> ranges.encloses(timestampRangeToTest))
                .orElse(false);
    }

    private CheckAndSetRequest getCheckAndSetRequest(Optional<ReadResult> oldValue, Range<Long> timestampRangeToAdd) {
        byte[] serializedTargetSet = serializeTimestampRangeSet(
                getTargetSet(oldValue.map(ReadResult::timestampRangeSet), timestampRangeToAdd));

        if (oldValue.isEmpty()) {
            return CheckAndSetRequest.newCell(tableReference, valueCell, serializedTargetSet);
        }
        return CheckAndSetRequest.singleCell(
                tableReference, valueCell, oldValue.get().valueReadFromDatabase(), serializedTargetSet);
    }

    private byte[] serializeTimestampRangeSet(TimestampRangeSet targetSet) {
        try {
            return OBJECT_MAPPER.writeValueAsBytes(targetSet);
        } catch (JsonProcessingException e) {
            log.warn("Error serializing timestamp range set", SafeArg.of("targetSet", targetSet), e);
            throw new RuntimeException(e);
        }
    }

    private TimestampRangeSet getTargetSet(Optional<TimestampRangeSet> originalSet, Range<Long> timestampRangeToAdd) {
        if (originalSet.isEmpty()) {
            return TimestampRangeSet.singleRange(timestampRangeToAdd);
        }
        return originalSet.get().copyAndAdd(timestampRangeToAdd);
    }

    private Optional<ReadResult> getInternal() {
        Map<Cell, Value> read = keyValueService.get(tableReference, ImmutableMap.of(valueCell, Long.MAX_VALUE));
        return read.values().stream().findAny().map(Value::getContents).map(bytes -> ImmutableReadResult.builder()
                .valueReadFromDatabase(bytes)
                .build());
    }

    @org.immutables.value.Value.Immutable
    interface ReadResult {
        byte[] valueReadFromDatabase();

        @org.immutables.value.Value.Lazy
        default TimestampRangeSet timestampRangeSet() {
            try {
                return OBJECT_MAPPER.readValue(valueReadFromDatabase(), TimestampRangeSet.class);
            } catch (IOException e) {
                log.warn(
                        "Error occurred when deserializing a timestamp range-set from the database",
                        SafeArg.of("value", valueReadFromDatabase()),
                        e);
                throw new RuntimeException(e);
            }
        }
    }
}

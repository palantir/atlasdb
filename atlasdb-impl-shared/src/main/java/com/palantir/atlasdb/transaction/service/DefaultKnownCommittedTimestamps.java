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

package com.palantir.atlasdb.transaction.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.common.concurrent.CoalescingSupplier;
import com.palantir.conjure.java.serialization.ObjectMappers;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

public final class DefaultKnownCommittedTimestamps implements KnownCommittedTimestamps {
    private static final ObjectMapper OBJECT_MAPPER = ObjectMappers.newSmileServerObjectMapper();
    private static final Cell MAGIC_CELL = Cell.create(PtBytes.toBytes("r"), PtBytes.toBytes("c"));

    private final KeyValueService keyValueService;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    // A bit naughty. Need to clean up synchronization
    private final CoalescingSupplier<byte[]> databaseMapLoader = new CoalescingSupplier<>(this::loadMapFromDatabase);

    @GuardedBy("lock")
    private SerializableTimestampSet knownCommittedTimestamps;

    public DefaultKnownCommittedTimestamps(KeyValueService keyValueService) {
        this.keyValueService = keyValueService;
        this.knownCommittedTimestamps =
                ImmutableSerializableTimestampSet.builder().build();
    }

    @Override
    public boolean isKnownCommitted(long timestamp) {
        ReadLock readLock = lock.readLock();
        readLock.lock();
        try {
            if (shouldLoadMapFromDatabase(timestamp)) {
                ensureMapLoadedFromDatabase();
            }
            return knownCommittedTimestamps.rangeSetView().contains(timestamp);
        } finally {
            readLock.unlock();
        }
    }

    // Naughty way of returning 2 values!
    private synchronized byte[] ensureMapLoadedFromDatabase() {
        try {
            byte[] value = databaseMapLoader.get();
            knownCommittedTimestamps = OBJECT_MAPPER.readValue(value, SerializableTimestampSet.class);
            return value;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] loadMapFromDatabase() {
        Map<Cell, Value> dbRead = keyValueService.get(
                TransactionConstants.KNOWN_COMMITTED_TIMESTAMPS,
                ImmutableMap.of(MAGIC_CELL, AtlasDbConstants.TRANSACTION_TS));
        return dbRead.get(MAGIC_CELL).getContents();
    }

    private boolean shouldLoadMapFromDatabase(long timestamp) {
        if (knownCommittedTimestamps.longRanges().isEmpty()) {
            return true;
        }

        // Bad Demeter. This logic should be in SerializableTimestampSet in the prod version
        Range<Long> highestRange = knownCommittedTimestamps
                .rangeSetView()
                .asDescendingSetOfRanges()
                .iterator()
                .next();
        Preconditions.checkState(
                highestRange.hasUpperBound(), "Infinite ranges not allowed in KnownCommittedTimestamps");
        long maxBound = highestRange.upperBoundType() == BoundType.CLOSED
                ? highestRange.upperEndpoint()
                : highestRange.upperEndpoint() - 1;
        return timestamp > maxBound;
    }

    @Override
    public void addInterval(Range<Long> knownCommittedInterval) {
        WriteLock writeLock = lock.writeLock();
        writeLock.lock();
        try {
            // update the DB first and then update local state
            // if DB update fails, local state is not updated
            // DB may be ahead of local state, that is OK
            while (true) {
                byte[] oldValue = ensureMapLoadedFromDatabase();

                if (knownCommittedTimestamps.rangeSetView().encloses(knownCommittedInterval)) {
                    return;
                }

                // This is responsible for coalescing
                SerializableTimestampSet withNewInterval =
                        SerializableTimestampSet.copyWithRange(knownCommittedTimestamps, knownCommittedInterval);
                byte[] newValue;
                try {
                    newValue = OBJECT_MAPPER.writeValueAsBytes(withNewInterval);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }

                try {
                    if (knownCommittedTimestamps.longRanges().isEmpty()) {
                        // Put new cell in
                        keyValueService.checkAndSet(CheckAndSetRequest.newCell(
                                TransactionConstants.KNOWN_COMMITTED_TIMESTAMPS, MAGIC_CELL, newValue));
                    } else {
                        keyValueService.checkAndSet(CheckAndSetRequest.singleCell(
                                TransactionConstants.KNOWN_COMMITTED_TIMESTAMPS, MAGIC_CELL, oldValue, newValue));
                    }

                    // DB write success!
                    knownCommittedTimestamps = withNewInterval;
                    return;
                } catch (CheckAndSetException checkAndSetException) {
                    // DB updated under us. If the DB value contains our thing we're ok
                    byte[] actualValue = Iterables.getOnlyElement(checkAndSetException.getActualValues());

                    try {
                        SerializableTimestampSet inDbSet =
                                OBJECT_MAPPER.readValue(actualValue, SerializableTimestampSet.class);
                        if (inDbSet.rangeSetView().enclosesAll(withNewInterval.rangeSetView())) {
                            knownCommittedTimestamps = inDbSet;
                            return;
                        }
                        // Need to try the whole CAS process again
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        } finally {
            writeLock.unlock();
        }
    }
}

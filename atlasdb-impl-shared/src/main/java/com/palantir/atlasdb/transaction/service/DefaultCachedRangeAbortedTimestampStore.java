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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.conjure.java.serialization.ObjectMappers;
import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;

public class DefaultCachedRangeAbortedTimestampStore implements CachedRangeAbortedTimestampStore {
    private static final ObjectMapper OBJECT_MAPPER = ObjectMappers.newSmileServerObjectMapper();
    private static final Cell MAGIC_CELL = Cell.create(PtBytes.toBytes("r"), PtBytes.toBytes("c"));

    private final KeyValueService keyValueService;
    private final Supplier<RangeSet<Long>> abortedRangesSupplier = Suppliers.memoize(this::loadAbortedRanges);

    public DefaultCachedRangeAbortedTimestampStore(KeyValueService keyValueService) {
        this.keyValueService = keyValueService;
    }

    @Override
    public boolean isTimestampAborted(long timestamp) {
        return abortedRangesSupplier.get().contains(timestamp);
    }

    @Override
    public void abortRangeOfTimestamps(Range<Long> timestampRange) {
        // TODO (jkong): Implement something like what we have in KnownCommittedTimestamps
        // It may be worth unifying some of the behaviour
    }

    private RangeSet<Long> loadAbortedRanges() {
        Map<Cell, Value> dbRead = keyValueService.get(
                TransactionConstants.KNOWN_ABORTED_RANGES,
                ImmutableMap.of(MAGIC_CELL, AtlasDbConstants.TRANSACTION_TS));
        if (dbRead.isEmpty()) {
            return TreeRangeSet.create();
        }
        byte[] serializedValue = dbRead.get(MAGIC_CELL).getContents();
        SerializableTimestampSet timestampSet;
        try {
            timestampSet = OBJECT_MAPPER.readValue(serializedValue, SerializableTimestampSet.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return timestampSet.rangeSetView();
    }
}

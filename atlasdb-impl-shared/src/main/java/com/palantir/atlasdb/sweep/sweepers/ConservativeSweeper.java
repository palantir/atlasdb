/**
 * Copyright 2016 Palantir Technologies
 * <p>
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://opensource.org/licenses/BSD-3-Clause
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.sweep.sweepers;

import java.util.Set;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;

public class ConservativeSweeper implements Sweeper {
    private final KeyValueService keyValueService;
    private final Supplier<Long> immutableTimestampSupplier;
    private final Supplier<Long> unreadableTimestampSupplier;

    public ConservativeSweeper(
            KeyValueService keyValueService,
            Supplier<Long> immutableTimestampSupplier,
            Supplier<Long> unreadableTimestampSupplier) {
        this.keyValueService = keyValueService;
        this.immutableTimestampSupplier = immutableTimestampSupplier;
        this.unreadableTimestampSupplier = unreadableTimestampSupplier;
    }

    @Override
    public long getSweepTimestamp() {
        return Math.min(unreadableTimestampSupplier.get(), immutableTimestampSupplier.get());
    }

    @Override
    public ClosableIterator<RowResult<Value>> getValues(TableReference table, RangeRequest range, long ts) {
        return ClosableIterators.emptyImmutableClosableIterator();
    }

    @Override
    public ClosableIterator<RowResult<Set<Long>>> getCellTimestamps(TableReference table, RangeRequest range, long ts) {
        return keyValueService.getRangeOfTimestamps(table, range, ts);
    }

    @Override
    public Set<Long> getTimestampsToIgnore() {
        return ImmutableSet.of(Value.INVALID_VALUE_TIMESTAMP);
    }

    @Override
    public boolean shouldAddSentinels() {
        return true;
    }
}

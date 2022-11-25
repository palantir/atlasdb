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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import org.junit.Test;

@SuppressWarnings("UnstableApiUsage") // RangeSet usage
public class KnownConcludedTransactionsStoreTest {
    private final KeyValueService keyValueService = new InMemoryKeyValueService(true);
    private final KnownConcludedTransactionsStore knownConcludedTransactionsStore =
            KnownConcludedTransactionsStore.create(keyValueService);

    @Test
    public void storeBeginsEmpty() {
        assertThat(knownConcludedTransactionsStore.get()).isEmpty();
    }

    @Test
    public void canRetrieveStoredRange() {
        knownConcludedTransactionsStore.supplement(Range.closedOpen(1L, 100L));
        assertThat(knownConcludedTransactionsStore.get())
                .contains(ConcludedRangeState.singleRange(
                        Range.closedOpen(1L, 100L), TransactionConstants.LOWEST_POSSIBLE_START_TS));
    }

    @Test
    public void coalescesRangesBeforeStorage() {
        knownConcludedTransactionsStore.supplement(Range.closedOpen(1L, 100L));
        knownConcludedTransactionsStore.supplement(Range.closedOpen(50L, 200L));
        assertThat(knownConcludedTransactionsStore.get())
                .contains(ConcludedRangeState.singleRange(
                        Range.closedOpen(1L, 200L), TransactionConstants.LOWEST_POSSIBLE_START_TS));
    }

    @Test
    public void tracksDistinctRanges() {
        knownConcludedTransactionsStore.supplement(Range.closedOpen(1L, 100L));
        knownConcludedTransactionsStore.supplement(Range.closedOpen(150L, 200L));
        assertThat(knownConcludedTransactionsStore.get())
                .contains(ImmutableConcludedRangeState.builder()
                        .timestampRanges(ImmutableRangeSet.<Long>builder()
                                .add(Range.closedOpen(1L, 100L))
                                .add(Range.closedOpen(150L, 200L))
                                .build())
                        .minimumConcludeableTimestamp(TransactionConstants.LOWEST_POSSIBLE_START_TS)
                        .build());
    }
}

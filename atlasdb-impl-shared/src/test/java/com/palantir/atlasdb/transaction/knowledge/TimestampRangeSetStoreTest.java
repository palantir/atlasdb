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
import org.junit.Test;

@SuppressWarnings("UnstableApiUsage") // RangeSet usage
public class TimestampRangeSetStoreTest {
    private final KeyValueService keyValueService = new InMemoryKeyValueService(true);
    private final TimestampRangeSetStore timestampRangeSetStore = TimestampRangeSetStore.create(keyValueService);

    @Test
    public void storeBeginsEmpty() {
        assertThat(timestampRangeSetStore.get()).isEmpty();
    }

    @Test
    public void canRetrieveStoredRange() {
        timestampRangeSetStore.supplement(Range.closedOpen(1L, 100L));
        assertThat(timestampRangeSetStore.get()).contains(TimestampRangeSet.singleRange(Range.closedOpen(1L, 100L)));
    }

    @Test
    public void coalescesRangesBeforeStorage() {
        timestampRangeSetStore.supplement(Range.closedOpen(1L, 100L));
        timestampRangeSetStore.supplement(Range.closedOpen(50L, 200L));
        assertThat(timestampRangeSetStore.get()).contains(TimestampRangeSet.singleRange(Range.closedOpen(1L, 200L)));
    }

    @Test
    public void tracksDistinctRanges() {
        timestampRangeSetStore.supplement(Range.closedOpen(1L, 100L));
        timestampRangeSetStore.supplement(Range.closedOpen(150L, 200L));
        assertThat(timestampRangeSetStore.get())
                .contains(ImmutableTimestampRangeSet.builder()
                        .timestampRanges(ImmutableRangeSet.<Long>builder()
                                .add(Range.closedOpen(1L, 100L))
                                .add(Range.closedOpen(150L, 200L))
                                .build())
                        .build());
    }
}

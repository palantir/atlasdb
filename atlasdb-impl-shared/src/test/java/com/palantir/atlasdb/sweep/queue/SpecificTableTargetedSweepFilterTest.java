/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.sweep.queue;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

public class SpecificTableTargetedSweepFilterTest {
    private static final TableReference TABLE_TO_FILTER_OUT = TableReference.createFromFullyQualifiedName("bad.evil");
    private static final TableReference TABLE_TO_KEEP = TableReference.createFromFullyQualifiedName("good.nice");

    private static final SpecificTableTargetedSweepFilter FILTER =
            new SpecificTableTargetedSweepFilter(() -> ImmutableSet.of(TABLE_TO_FILTER_OUT));

    private static final Cell CELL = Cell.create(PtBytes.toBytes("123"), PtBytes.toBytes("456"));
    private static final WriteInfo WRITE_TO_BAD_TABLE = WriteInfo.write(TABLE_TO_FILTER_OUT, CELL, 42L);
    private static final WriteInfo WRITE_TO_GOOD_TABLE = WriteInfo.write(TABLE_TO_KEEP, CELL, 42L);

    @Test
    public void filtersOutSpecifiedTableReferences() {
        assertThat(FILTER.filter(ImmutableSet.of(WRITE_TO_BAD_TABLE, WRITE_TO_GOOD_TABLE)))
                .containsExactlyInAnyOrder(WRITE_TO_GOOD_TABLE);
    }

    @Test
    public void liveReloadsTableReferences() {
        AtomicReference<Set<TableReference>> tableToFilterOut = new AtomicReference<>(ImmutableSet.of());
        SpecificTableTargetedSweepFilter changingFilter = new SpecificTableTargetedSweepFilter(tableToFilterOut::get);

        assertThat(changingFilter.filter(ImmutableSet.of(WRITE_TO_BAD_TABLE, WRITE_TO_GOOD_TABLE)))
                .containsExactlyInAnyOrder(WRITE_TO_BAD_TABLE, WRITE_TO_GOOD_TABLE);

        tableToFilterOut.set(ImmutableSet.of(TABLE_TO_FILTER_OUT));
        assertThat(changingFilter.filter(ImmutableSet.of(WRITE_TO_BAD_TABLE, WRITE_TO_GOOD_TABLE)))
                .containsExactlyInAnyOrder(WRITE_TO_GOOD_TABLE);
    }
}

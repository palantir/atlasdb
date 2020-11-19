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
import com.google.common.collect.Sets;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import org.junit.Test;

public class CompoundTargetedSweepFilterTest {
    private static final TableReference TABLE_REFERENCE = TableReference.createFromFullyQualifiedName("table.one");
    private static final WriteInfo WRITE_INFO_1 =
            WriteInfo.write(TABLE_REFERENCE, Cell.create(PtBytes.toBytes("fire"), PtBytes.toBytes("water")), 42L);
    private static final WriteInfo WRITE_INFO_2 =
            WriteInfo.write(TABLE_REFERENCE, Cell.create(PtBytes.toBytes("order"), PtBytes.toBytes("chaos")), 42L);
    private static final WriteInfo WRITE_INFO_3 =
            WriteInfo.write(TABLE_REFERENCE, Cell.create(PtBytes.toBytes("apple"), PtBytes.toBytes("banana")), 42L);

    private final TargetedSweepFilter FILTER_OUT_1 =
            cellsToDelete -> Sets.difference(ImmutableSet.copyOf(cellsToDelete), ImmutableSet.of(WRITE_INFO_1));
    private final TargetedSweepFilter FILTER_OUT_2 =
            cellsToDelete -> Sets.difference(ImmutableSet.copyOf(cellsToDelete), ImmutableSet.of(WRITE_INFO_2));
    private final TargetedSweepFilter COMPOUND_FILTER =
            new CompoundTargetedSweepFilter(ImmutableSet.of(FILTER_OUT_1, FILTER_OUT_2));

    @Test
    public void appliesAllFilters() {
        assertThat(COMPOUND_FILTER.filter(ImmutableSet.of(WRITE_INFO_1, WRITE_INFO_2, WRITE_INFO_3)))
                .containsExactlyInAnyOrder(WRITE_INFO_3);
    }
}

/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.sweep;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;

import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Value;

public class CellsAndTimestampsShould {
    private static final Cell SINGLE_CELL = Cell.create(
            "cellRow".getBytes(StandardCharsets.UTF_8),
            "cellCol".getBytes(StandardCharsets.UTF_8));

    private static final long VALID_TIMESTAMP = 123L;

    @Test
    public void removeIgnoredTimestamps() {
        CellAndTimestamps itemWithNoTimestamps = CellAndTimestamps.of(SINGLE_CELL, ImmutableSet.of());
        CellAndTimestamps itemWithInvalidTimestamp =
                CellAndTimestamps.of(SINGLE_CELL, ImmutableSet.of(Value.INVALID_VALUE_TIMESTAMP));
        CellsAndTimestamps cellsToSweep = CellsAndTimestamps.withSingleItem(itemWithInvalidTimestamp);

        CellsAndTimestamps result = cellsToSweep.withoutIgnoredTimestamps(
                ImmutableSet.of(Value.INVALID_VALUE_TIMESTAMP));

        assertThat(result).isEqualTo(CellsAndTimestamps.withSingleItem(itemWithNoTimestamps));
    }

    @Test
    public void keepTimestampsThatAreNotIgnored() {
        CellAndTimestamps itemWithValidTimestamp = CellAndTimestamps.of(SINGLE_CELL, ImmutableSet.of(VALID_TIMESTAMP));
        CellsAndTimestamps cellsToSweep = CellsAndTimestamps.withSingleItem(itemWithValidTimestamp);

        CellsAndTimestamps result = cellsToSweep.withoutIgnoredTimestamps(ImmutableSet.of());

        assertThat(result).isEqualTo(cellsToSweep);
    }
}

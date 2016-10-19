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
package com.palantir.atlasdb.sweep;

import java.util.Optional;
import java.util.Set;

import org.immutables.value.Value;

import com.palantir.atlasdb.keyvalue.api.Cell;

@Value.Immutable
public abstract class CellToSweep {
    public static CellToSweep of(Cell cell, Set<Long> timestamps, boolean needsSentinel) {
        return ImmutableCellToSweep.builder()
                .cell(cell)
                .timestamps(timestamps)
                .needsSentinel(needsSentinel)
                .build();
    }

    public abstract Cell cell();
    public abstract Set<Long> timestamps();
    public abstract boolean needsSentinel();

    public Optional<Cell> sentinel() {
        if (needsSentinel()) {
            return Optional.of(cell());
        } else {
            return Optional.empty();
        }
    }
}

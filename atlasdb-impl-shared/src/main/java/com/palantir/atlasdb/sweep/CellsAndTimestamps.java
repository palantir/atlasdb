/**
 * Copyright 2016 Palantir Technologies
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

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.immutables.value.Value;

@Value.Immutable
public abstract class CellsAndTimestamps {
    public abstract List<CellAndTimestamps> cellAndTimestampsList();

    public static CellsAndTimestamps withSingleItem(CellAndTimestamps singleItem) {
        return ImmutableCellsAndTimestamps.builder()
                .addCellAndTimestampsList(singleItem)
                .build();
    }

    public Set<Long> getAllTimestampValues() {
        return cellAndTimestampsList().stream()
                .flatMap(cellAndTimestamps -> cellAndTimestamps.timestamps().stream())
                .collect(Collectors.toSet());
    }
}

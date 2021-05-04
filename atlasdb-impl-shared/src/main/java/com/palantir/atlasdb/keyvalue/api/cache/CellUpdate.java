/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.api.cache;

import com.palantir.atlasdb.keyvalue.api.CellReference;
import java.util.Set;
import org.immutables.value.Value;

interface CellUpdate {
    <T> T accept(Visitor<T> visitor);

    static CellUpdate invalidateAll() {
        return ImmutableInvalidateAll.builder().build();
    }

    static CellUpdate invalidateSome(Set<CellReference> invalidatedCells) {
        return ImmutableInvalidateSome.builder()
                .invalidatedCells(invalidatedCells)
                .build();
    }

    @Value.Immutable
    interface InvalidateAll extends CellUpdate {
        @Override
        default <T> T accept(Visitor<T> visitor) {
            return visitor.invalidateAll();
        }
    }

    @Value.Immutable
    interface InvalidateSome extends CellUpdate {
        Set<CellReference> invalidatedCells();

        @Override
        default <T> T accept(Visitor<T> visitor) {
            return visitor.invalidateSome(invalidatedCells());
        }
    }

    interface Visitor<T> {
        T invalidateAll();

        T invalidateSome(Set<CellReference> invalidatedCells);
    }
}

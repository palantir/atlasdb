/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.contiguous;

import com.palantir.atlasdb.keyvalue.api.Cell;
import java.util.List;
import java.util.SortedMap;

public final class ArbitraryContiguityCheckers {
    public enum CellChecker implements ContiguityChecker<Cell> {
        INSTANCE;

        @Override
        public boolean checkContiguityAndUpdateLastSeen(Cell element) {
            return false;
        }

        @Override
        public boolean checkContiguityAndUpdateLastSeen(List<Cell> elements) {
            return false;
        }

        @Override
        public <T2> boolean checkContiguityAndUpdateLastSeen(SortedMap<Cell, T2> iterator) {
            return false;
        }
    }
}

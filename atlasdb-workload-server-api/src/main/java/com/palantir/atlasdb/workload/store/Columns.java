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

package com.palantir.atlasdb.workload.store;

import io.vavr.collection.SortedMap;
import java.util.Optional;

public interface Columns {
    Optional<Integer> get(int column);

    boolean containsKey(int column);

    void put(int column, int value);

    void delete(int column);

    SortedMap<Integer, Optional<Integer>> getColumnsInRange(
            Optional<Integer> startInclusive, Optional<Integer> endExclusive);

    SortedMap<Integer, Optional<Integer>> snapshot();
}

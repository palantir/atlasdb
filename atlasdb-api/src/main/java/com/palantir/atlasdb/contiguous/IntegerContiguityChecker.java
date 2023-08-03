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

import java.util.List;
import java.util.SortedMap;

public class IntegerContiguityChecker implements ContiguityChecker<Integer> {
    private final GenericContiguityChecker<Integer> delegate;

    public IntegerContiguityChecker(int initialValue) {
        this.delegate = new GenericContiguityChecker<>(initialValue, Math::max, (a, b) -> a + 1 == b, Integer::sum);
    }

    @Override
    public boolean checkContiguityAndUpdateLastSeen(Integer element) {
        return delegate.checkContiguityAndUpdateLastSeen(element);
    }

    @Override
    public boolean checkContiguityAndUpdateLastSeen(List<Integer> elements) {
        return delegate.checkContiguityAndUpdateLastSeen(elements);
    }

    @Override
    public <T2> boolean checkContiguityAndUpdateLastSeen(SortedMap<Integer, T2> map) {
        return delegate.checkContiguityAndUpdateLastSeen(map);
    }
}

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

import com.palantir.logsafe.Preconditions;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;

public class GenericContiguityChecker<T> implements ContiguityChecker<T> {
    private final AtomicReference<T> upperBound;

    private final BinaryOperator<T> mergeFunction;
    private final BiPredicate<T, T> isFirstJustBelowSecond;
    private final BiFunction<T, Integer, T> additionFunction;

    public GenericContiguityChecker(
            T initialValue,
            BinaryOperator<T> mergeFunction,
            BiPredicate<T, T> isFirstJustBelowSecond,
            BiFunction<T, Integer, T> additionFunction) {
        this.upperBound = new AtomicReference<>(initialValue);
        this.mergeFunction = mergeFunction;
        this.isFirstJustBelowSecond = isFirstJustBelowSecond;
        this.additionFunction = additionFunction;
    }

    @Override
    public boolean checkContiguityAndUpdateLastSeen(T element) {
        T previousValue = upperBound.getAndAccumulate(element, mergeFunction);
        if (previousValue == null) {
            return false;
        }
        return isFirstJustBelowSecond.test(previousValue, element);
    }

    @Override
    public boolean checkContiguityAndUpdateLastSeen(List<T> elements) {
        Preconditions.checkState(!elements.isEmpty(), "Cannot check contiguity of an empty list");
        T previousValue = upperBound.getAndAccumulate(elements.get(elements.size() - 1), mergeFunction);
        return listIsContiguous(elements) && isFirstJustBelowSecond.test(elements.get(0), previousValue);
    }

    @Override
    public <T2> boolean checkContiguityAndUpdateLastSeen(SortedMap<T, T2> map) {
        Preconditions.checkState(!map.isEmpty(), "Cannot check contiguity of an empty map");
        T previousValue = upperBound.getAndAccumulate(map.lastKey(), mergeFunction);
        return mapIsContiguous(map) && isFirstJustBelowSecond.test(map.firstKey(), previousValue);
    }

    private boolean listIsContiguous(List<T> elements) {
        return additionFunction.apply(elements.get(0), elements.size() - 1).equals(elements.get(elements.size() - 1));
    }

    private <T2> boolean mapIsContiguous(SortedMap<T, T2> map) {
        return additionFunction.apply(map.firstKey(), map.size() - 1).equals(map.lastKey());
    }
}

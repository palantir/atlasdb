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

/**
 * A class that validates that a given value, or set of values, is contiguous. This can accept values one at a time.
 * <p>
 * The assumption made by the contiguity checker is that the maximal previously seen element IS sufficient to determine
 * contiguity. In particular, using the domain of integers and the conventional definition of contiguity, the following
 * are expected:
 * <p>
 * checkContiguityAndUpdateLastSeen(5); // true
 * checkContiguityAndUpdateLastSeen(6, 8); // false: 7 is missing
 * checkContiguityAndUpdateLastSeen(9, 10); // true: this is contiguous wrt 8
 * checkContiguityAndUpdateLastSeen(7): // false: while 7 is now present, this is awkward
 */
public interface ContiguityChecker<T> {
    // TODO (jkong): Support varargs and related variants
    // TODO (jkong): Support primitives

    boolean checkContiguityAndUpdateLastSeen(T element);

    // Precondition: elements is sorted
    boolean checkContiguityAndUpdateLastSeen(List<T> elements);

    <T2> boolean checkContiguityAndUpdateLastSeen(SortedMap<T, T2> map);
}

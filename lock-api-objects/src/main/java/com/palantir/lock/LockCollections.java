/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.lock;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.palantir.logsafe.Preconditions;
import java.util.Comparator;
import java.util.Map;
import java.util.SortedMap;

public final class LockCollections {

    private LockCollections() {
        // cannot instantiate
    }

    public static <T> LockCollection<T> of(Map<T, LockMode> locks) {
        return new LockCollection<T>(locks.entrySet());
    }

    public static <T extends Comparable<T>> SortedLockCollection<T> of(SortedMap<T, LockMode> locks) {
        Preconditions.checkArgument(
                locks.comparator() == null
                        || locks.comparator() == Ordering.natural()
                        || locks.comparator() == Comparator.naturalOrder(),
                "sorted lock collections must use naturally comparable keys");
        return new SortedLockCollection<T>(locks.entrySet());
    }

    public static <T> SortedLockCollection<T> of() {
        return new SortedLockCollection<T>(ImmutableList.<Map.Entry<T, LockMode>>of());
    }
}

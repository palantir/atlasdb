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

import com.google.common.collect.Iterables;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Replacement for when you would normally use SortedMap&lt;T, LockMode&gt;,
 * but don't actually need the keys to be a set, or to have random
 * lookup of values.
 */
public class SortedLockCollection<T> extends LockCollection<T> implements Set<T> {
    private static final long serialVersionUID = 1L;

    SortedLockCollection(Collection<Map.Entry<T, LockMode>> locks) {
        super(locks);
    }

    @Override
    public String toString() {
        return "SortedLockCollection " + Iterables.toString(entries());
    }

    @Override
    public boolean contains(Object obj) {
        return Arrays.binarySearch(keys, obj) >= 0;
    }
}

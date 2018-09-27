/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.transaction.client;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

// TODO (jkong): Replace with a better implementation
public class NaiveModulusAllocator<T> implements ModulusAllocator<T> {
    private final int modulus;
    private final Map<T, Integer> history;
    private final AtomicInteger assigner;

    private NaiveModulusAllocator(int modulus, Map<T, Integer> history, AtomicInteger assigner) {
        this.modulus = modulus;
        this.history = history;
        this.assigner = assigner;
    }

    public NaiveModulusAllocator(int modulus) {
        this(modulus, Maps.newHashMap(), new AtomicInteger());
    }

    @Override
    public synchronized List<Integer> getRelevantModuli(T object) {
        return ImmutableList.of(
                history.computeIfAbsent(
                        object,
                        (unused) -> assigner.updateAndGet(residue -> (residue + 1) % modulus)));
    }
}

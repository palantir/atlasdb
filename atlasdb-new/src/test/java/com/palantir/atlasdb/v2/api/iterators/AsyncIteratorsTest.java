/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.v2.api.iterators;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Iterator;

import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.palantir.atlasdb.v2.api.api.AsyncIterator;

public class AsyncIteratorsTest {
    private static final String THROW = "throw";
    private final AsyncIterators iterators = new AsyncIterators(MoreExecutors.directExecutor());
    private final Iterator<String> iterator = ImmutableSet.of("something", THROW).iterator();
    private final AsyncIterator<String> async = new NonBlockingIterator<>(iterator);

    @Test
    public void testForEachFailsIfElementFails() {
        RuntimeException toThrow = new RuntimeException();
        assertThatThrownBy(() -> Futures.getUnchecked(iterators.forEach(async, element -> {
            if (element.equals(THROW)) {
                throw toThrow;
            }
        }))).isInstanceOf(UncheckedExecutionException.class).hasCause(toThrow);
    }
}
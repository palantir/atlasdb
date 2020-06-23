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

package com.palantir.atlasdb.transaction.impl;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;

public final class UnstableOrderedIterable<T> implements Iterable<T> {
    private final Iterator<List<T>> orderingIterator;

    private UnstableOrderedIterable(Iterator<List<T>> orderingIterator) {
        this.orderingIterator = orderingIterator;
    }

    public static <T> Iterable<T> create(Collection<T> underlying, Comparator<T> comparator) {
        return new UnstableOrderedIterable<T>(
                Iterables.cycle(Collections2.orderedPermutations(underlying, comparator)).iterator());
    }

    @Override
    public Iterator<T> iterator() {
        return orderingIterator.next().iterator();
    }
}

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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class UnstableOrderedIterable<T> implements Iterable<T> {
    private final List<T> underlying;

    public UnstableOrderedIterable(Collection<T> underlying) {
        this.underlying = ImmutableList.copyOf(underlying);
    }

    @Override
    public Iterator<T> iterator() {
        List<T> copy = Lists.newArrayList(underlying);
        Collections.shuffle(copy);
        return copy.iterator();
    }
}

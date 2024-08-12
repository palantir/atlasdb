/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.sweep.asts;

import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.concurrent.NotThreadSafe;

// We could use something similar to a Linear Congruential Generator to generate a pseudo-random sequence of
// numbers across the range [0, len(remainingBuckets))
// I'm not the first to think of this, see
// https://lemire.me/blog/2017/09/18/visiting-all-values-in-an-array-exactly-once-in-random-order/
// But, this is sufficient, and ideally, we'd just have an endpoint in Timelock that says give me the first unlocked
// thing in this set.
@NotThreadSafe
public final class ProbingRandomIterator<T> implements Iterator<T> {
    private final List<T> list;
    private final BitSet visited;
    private int probedElements = 0;
    private int lastIndex = 0;

    public ProbingRandomIterator(List<T> list) {
        this.list = list;
        this.visited = new BitSet(list.size());
    }

    @Override
    public boolean hasNext() {
        return probedElements < list.size();
    }

    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        int nextStart = (lastIndex + ThreadLocalRandom.current().nextInt(list.size())) % list.size();
        int nextClearBit = visited.nextClearBit(nextStart);
        if (nextClearBit >= list.size()) {
            nextClearBit = visited.nextClearBit(0);
        }
        lastIndex = nextClearBit;
        visited.set(lastIndex);
        probedElements++;
        return list.get(lastIndex);
    }
}

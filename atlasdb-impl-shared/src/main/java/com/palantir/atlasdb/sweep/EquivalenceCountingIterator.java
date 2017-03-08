/**
 * Copyright 2017 Palantir Technologies
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
package com.palantir.atlasdb.sweep;

import java.util.Iterator;
import java.util.NoSuchElementException;

import com.google.common.base.Equivalence;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

/**
 * This iterator counts elements as it iterates through them, without incrementing the count for successive occurrences
 * of elements that are equivalent as defined in the equivalence relation. Calls to hasNext() and next() will fail
 * once the next row would be the (limit + 1)th unique element.
 *
 * In particular, if all occurrences of each equivalence class are grouped together (as is the case when the elements
 * are sorted), this allows us to iterate through the first limit classes only.
 */
public class EquivalenceCountingIterator<T> extends CountingIterator<T> {
    private final int limit;
    Equivalence<T> equivalence;

    /**
     * Constructor.
     *
     * @param delegate Iterator to use
     * @param limit The iterator reaches the end if next() would cause the counter to exceed limit
     * @param equivalence Definition of equivalence for the purpose of ignoring successive elements
     */
    public EquivalenceCountingIterator(Iterator<T> delegate, int limit, Equivalence<T> equivalence) {
        super(Iterators.peekingIterator(delegate));
        this.limit = limit;
        this.equivalence = equivalence;
    }

    @Override
    public boolean hasNext() {
        if (delegate.hasNext()) {
            if (limit >= totalItems + returnOneIfNextRowIsNew()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException((totalItems == limit) ? "Reached limit" : "End of delegate iterator");
        }
        T nextItem = delegate.next();
        if (!equivalence.equivalent(lastItem, nextItem)) {
            totalItems++;
        }
        lastItem = nextItem;
        return nextItem;
    }

    private int returnOneIfNextRowIsNew() {
        T nextItem = ((PeekingIterator<T>) delegate).peek();
        if (!equivalence.equivalent(lastItem, nextItem)) {
            return 1;
        }
        return 0;
    }

    @Override
    public void remove() {
    // not implemented
    }
}




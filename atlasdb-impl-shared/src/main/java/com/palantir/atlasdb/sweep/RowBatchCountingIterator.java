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

import java.util.Arrays;
import java.util.Iterator;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.palantir.atlasdb.keyvalue.api.RowResult;

/**
 * This iterator counts unique rows that it iterates through. Calls to hasNext() and next() will fail once the next
 * row would be the (rowBatchSize + 1)th unique row.
 */
public class RowBatchCountingIterator<T> extends CountingIterator<RowResult<T>> {
    private final int rowBatchSize;
    byte[] currentRowName = null;

    public RowBatchCountingIterator(Iterator<RowResult<T>> delegate, int rowBatchSize) {
        super(Iterators.peekingIterator(delegate));
        this.rowBatchSize = rowBatchSize;
    }

    @Override
    public boolean hasNext() {
        if (delegate.hasNext()) {
            if (rowBatchSize >= totalItems + returnOneIfNextRowIsNew()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public RowResult<T> next() {
        RowResult<T> next = delegate.next();
        lastItem = next;
        if (!Arrays.equals(currentRowName, lastItem.getRowName())) {
            totalItems++;
            currentRowName = lastItem.getRowName();
        }
        return next;
    }

    private int returnOneIfNextRowIsNew() {
        byte[] nextRowName = ((PeekingIterator<RowResult<T>>) delegate).peek().getRowName();
        if (!Arrays.equals(currentRowName, nextRowName)) {
            return 1;
        }
        return 0;
    }

    @Override
    public void remove() {
    // not implemented
    }
}

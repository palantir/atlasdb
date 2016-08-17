/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.partition.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.partition.exception.VersionMismatchException;
import com.palantir.common.base.ClosableIterator;

/**
 * The purpose of this class is to have range requests not throw version mismatch exceptions.
 * Instead this exception will be caught by this wrapping iterator and a new range iterator
 * will be requested from the PartitionedKeyValueService.
 *
 * We need this, because range requests are sometimes used without a transaction manager and
 * thus are not retried but instead cause a crash if version mismatch exception is thrown.
 *
 * @author htarasiuk
 */
public final class AutoRetryingClosableIterator<T> implements ClosableIterator<RowResult<T>> {
    private static final Logger log = LoggerFactory.getLogger(AutoRetryingClosableIterator.class);

    private final RangeRequest originalRange;
    private final Function<RangeRequest, ClosableIterator<RowResult<T>>> backingIteratorSupplier;
    private ClosableIterator<RowResult<T>> backingIterator;
    private byte[] lastRow = null;

    // This will make a range request that can be used to continue
    // the range iteration with a new iterator.
    private RangeRequest makeNewRange() {
        // No single element was retrieved so far
        if (lastRow == null) {
            return originalRange;
        }

        // All elements were already retrieved and the last one
        // was a terminal row
        if (RangeRequests.isTerminalRow(originalRange.isReverse(), lastRow)) {
            // Return an empty range keeping other properties of the requests
            return originalRange.getBuilder()
                    .startRowInclusive(RangeRequests.getFirstRowName())
                    .endRowExclusive(RangeRequests.getFirstRowName())
                    .build();
        }

        // Some elements were already retrieved and the last one was
        // NOT a terminal row
        byte[] newStartRow = RangeRequests.getNextStartRow(originalRange.isReverse(), lastRow);
        return originalRange.getBuilder().startRowInclusive(newStartRow).build();
    }

    private void requestNewIteratorAfterVersionMismatch() {
        try {
            backingIterator.close();
        } catch (RuntimeException e) {
            log.warn("Error while closing outdated range iterator:");
            e.printStackTrace(System.out);
        }
        backingIterator = backingIteratorSupplier.apply(makeNewRange());
    }

    private <V> V runTaskWithRetry(Function<Void, V> task) {
        while (true) {
            try {
                return task.apply(null);
            } catch (VersionMismatchException e) {
                requestNewIteratorAfterVersionMismatch();
            }
        }
    }

    private AutoRetryingClosableIterator(RangeRequest originalRange,
            Function<RangeRequest, ClosableIterator<RowResult<T>>> backingIteratorSupplier) {
        this.originalRange = originalRange;
        this.backingIteratorSupplier = backingIteratorSupplier;
        this.backingIterator = backingIteratorSupplier.apply(originalRange);
    }

    public static <T> AutoRetryingClosableIterator<T> of(RangeRequest originalRange,
            Function<RangeRequest, ClosableIterator<RowResult<T>>> backingIteratorSupplier) {
        return new AutoRetryingClosableIterator<>(originalRange, backingIteratorSupplier);
    }

    @Override
    public boolean hasNext() {
        return runTaskWithRetry(new Function<Void, Boolean>() {
            @Override
            public Boolean apply(Void input) {
                return backingIterator.hasNext();
            }
        });
    }

    @Override
    public RowResult<T> next() {
        RowResult<T> ret = runTaskWithRetry(new Function<Void, RowResult<T>>() {
            @Override
            public RowResult<T> apply(Void input) {
                return backingIterator.next();
            }
        });
        lastRow = ret.getRowName();
        return ret;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        backingIterator.close();
    }

}

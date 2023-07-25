/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl.expectations;

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.cache.CacheValue;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.Arrays;
import java.util.Map;
import one.util.streamex.EntryStream;

public final class ExpectedCellsContainingValueValidator {

    private ExpectedCellsContainingValueValidator() {}

    public static long validateCacheAndGetNonEmptyValuesCount(
            long expectedNumberOfPresentCellsToFetch, Map<Cell, CacheValue> cachedLookup) {
        Map<Cell, byte[]> cachedCellsWithNonEmptyValue = EntryStream.of(cachedLookup)
                .filterValues(value -> value.value().isPresent()
                        && !Arrays.equals(value.value().get(), PtBytes.EMPTY_BYTE_ARRAY))
                .mapValues(value -> value.value().get())
                .toMap();

        validateFetchedLessOrEqualToExpected(expectedNumberOfPresentCellsToFetch, cachedCellsWithNonEmptyValue);

        return cachedCellsWithNonEmptyValue.size();
    }

    public static void validateFetchedLessOrEqualToExpected(
            long expectedNumberOfPresentCellsToFetch, Map<Cell, byte[]> fetchedCells) {
        if (fetchedCells.size() > expectedNumberOfPresentCellsToFetch) {
            throw new MoreCellsPresentThanExpectedException(fetchedCells, expectedNumberOfPresentCellsToFetch);
        }
    }

    public static class MoreCellsPresentThanExpectedException extends IllegalStateException {
        private final Map<Cell, byte[]> fetchedCells;

        public MoreCellsPresentThanExpectedException(Map<Cell, byte[]> fetchedCells, long expectedNumberOfCells) {
            super(new SafeIllegalStateException(
                    "KeyValueService returned more results than Get expected. This means there is a bug"
                            + "either in the SnapshotTransaction implementation or in how the client is "
                            + "using such method.",
                    SafeArg.of("expectedNumberOfCells", expectedNumberOfCells),
                    SafeArg.of("numberOfCellsRetrieved", fetchedCells.size()),
                    UnsafeArg.of("retrievedCells", fetchedCells)));
            this.fetchedCells = fetchedCells;
        }

        public Map<Cell, byte[]> getFetchedCells() {
            return fetchedCells;
        }
    }
}

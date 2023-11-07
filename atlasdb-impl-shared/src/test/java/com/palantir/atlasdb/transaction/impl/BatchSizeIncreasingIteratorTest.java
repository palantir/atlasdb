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

package com.palantir.atlasdb.transaction.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.AtlasDbPerformanceConstants;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

public class BatchSizeIncreasingIteratorTest {

    @Test
    public void handlesIntegerOverflow() {
        assertThat(new BatchSizeIncreasingIterator<>(
                                new TestBatchProvider(0),
                                Integer.MAX_VALUE,
                                ClosableIterators.emptyImmutableClosableIterator())
                        .getMaxBatchSize())
                .isEqualTo(AtlasDbPerformanceConstants.MAX_BATCH_SIZE);
    }

    @Test
    public void getBestBatchSizeEqualToOriginalOnFirstIteration() {
        assertThat(new BatchSizeIncreasingIterator<>(
                                new TestBatchProvider(0), 1, ClosableIterators.emptyImmutableClosableIterator())
                        .getBestBatchSize())
                .isEqualTo(1);
    }

    @Test
    public void getBestBatchSizeNeverLargerThanMax() {
        int originalBatchSize = 1;
        BatchSizeIncreasingIterator<Integer> iterator = new BatchSizeIncreasingIterator<>(
                new TestBatchProvider(1000),
                originalBatchSize,
                ClosableIterators.wrap(List.of(0).iterator()));
        // This will make the batch size increasing iterator think that it is only getting deleted values, which will
        // result in an increase by BatchSizeIncreasingIterator.INCREASE_FACTOR.
        iterator.markNumResultsNotDeleted(0);
        IntStream.range(0, BatchSizeIncreasingIterator.MAX_FACTOR / BatchSizeIncreasingIterator.INCREASE_FACTOR + 1)
                .forEach(_ignore -> iterator.getBatch());
        assertThat(iterator.getBestBatchSize()).isEqualTo(originalBatchSize * BatchSizeIncreasingIterator.MAX_FACTOR);
    }

    private static class TestBatchProvider implements BatchProvider<Integer> {

        private final NavigableSet<Integer> values;

        public TestBatchProvider(Integer count) {
            this(IntStream.range(0, count)
                    .boxed()
                    .collect(Collectors.<Integer, TreeSet<Integer>>toCollection(TreeSet::new)));
        }

        public TestBatchProvider(NavigableSet<Integer> values) {
            this.values = values;
        }

        @Override
        public ClosableIterator<Integer> getBatch(int batchSize, byte[] lastToken) {
            int value = ByteBuffer.wrap(lastToken).getInt();
            return ClosableIterators.wrapWithEmptyClose(
                    values.tailSet(value, true).iterator());
        }

        @Override
        public boolean hasNext(byte[] lastToken) {
            int value = ByteBuffer.wrap(lastToken).getInt();
            return values.ceiling(value) != null;
        }

        @Override
        public byte[] getLastToken(List<Integer> batch) {
            return ByteBuffer.allocate(Integer.BYTES)
                    .putInt(batch.get(batch.size() - 1))
                    .array();
        }
    }
}

/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

import java.util.Collections;
import java.util.List;

import javax.annotation.Nullable;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;

public class BatchSizeIncreasingIteratorTest {
    private final static class OneAtATimeBatchProvider implements BatchProvider<Integer> {
        private List<Integer> values;

        private OneAtATimeBatchProvider(List<Integer> values) {
            this.values = values;
        }

        @Override
        public ClosableIterator<Integer> getBatch(int batchSize, @Nullable byte[] lastToken) {
            int indexOfLastValue = lastToken == null ? -1 : values.indexOf(fromToken(lastToken));
            Integer nextValue = values.get(indexOfLastValue + 1);
            return ClosableIterators.wrap(Collections.singleton(nextValue).iterator());
        }

        @Override
        public boolean hasNext(byte[] lastToken) {
            return !values.get(values.size() - 1).equals(fromToken(lastToken));
        }

        @Override
        public byte[] getLastToken(List<Integer> batch) {
            return toToken(batch.get(batch.size() - 1));
        }

        private byte[] toToken(Integer integer) {
            return String.valueOf(integer).getBytes();
        }

        private Integer fromToken(byte[] token) {
            return Integer.parseInt(new String(token));
        }
    }

    @Test
    public void testAllAtOnce() {
        List<Integer> values = ImmutableList.of(0, 1, 2);
        BatchSizeIncreasingIterator<Integer> iterator = new BatchSizeIncreasingIterator<>(
                new OneAtATimeBatchProvider(values),
                10,
                ClosableIterators.wrap(values.iterator()));
        assertThat(iterator.getBatch()).isEqualTo(values);
        assertThat(iterator.getBatch()).isEmpty();
    }

    @Test
    public void testBatchOneAtATime() {
        List<Integer> values = ImmutableList.of(0, 1, 2);
        BatchSizeIncreasingIterator<Integer> iterator = new BatchSizeIncreasingIterator<>(
                new OneAtATimeBatchProvider(values),
                10,
                null);
        assertThat(iterator.getBatch()).containsExactly(0);
        assertThat(iterator.getBatch()).containsExactly(1);
        assertThat(iterator.getBatch()).containsExactly(2);
        assertThat(iterator.getBatch()).isEmpty();
    }
}

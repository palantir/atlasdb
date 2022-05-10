/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.impl.KvsManager;
import com.palantir.atlasdb.keyvalue.impl.TransactionManagerManager;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.common.base.BatchingVisitable;
import com.palantir.common.base.BatchingVisitables;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public abstract class AbstractTransactionTest extends TransactionTestSetup {
    private static final BatchColumnRangeSelection ALL_COLUMNS =
            BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 3);

    public AbstractTransactionTest(KvsManager kvsManager, TransactionManagerManager tmManager) {
        super(kvsManager, tmManager);
    }

    protected boolean supportsReverse() {
        return true;
    }

    // Duplicates of TransactionTestConstants since this is currently (incorrectly) in main
    // rather than test. Can use the former once we resolve the dependency issues.
    public static final int GET_RANGES_THREAD_POOL_SIZE = 16;
    public static final int DEFAULT_GET_RANGES_CONCURRENCY = 4;

    protected static final ExecutorService GET_RANGES_EXECUTOR =
            Executors.newFixedThreadPool(GET_RANGES_THREAD_POOL_SIZE);

    protected Transaction startTransaction() {
        return Iterables.getOnlyElement(txMgr.startTransactions(List.of(PreCommitConditions.NO_OP)));
    }

    private void verifyAllGetRangesImplsRangeSizes(
            Transaction tx, RangeRequest templateRangeRequest, int expectedRangeSize) {
        Iterable<RangeRequest> rangeRequests = Iterables.limit(Iterables.cycle(templateRangeRequest), 1000);

        List<BatchingVisitable<RowResult<byte[]>>> getRangesWithPrefetchingImpl =
                ImmutableList.copyOf(tx.getRanges(TEST_TABLE, rangeRequests));
        List<BatchingVisitable<RowResult<byte[]>>> getRangesInParallelImpl = tx.getRanges(
                        TEST_TABLE, rangeRequests, 2, (rangeRequest, visitable) -> visitable)
                .collect(Collectors.toList());
        List<BatchingVisitable<RowResult<byte[]>>> getRangesLazyImpl =
                tx.getRangesLazy(TEST_TABLE, rangeRequests).collect(Collectors.toList());

        assertThat(getRangesLazyImpl).hasSameSizeAs(getRangesWithPrefetchingImpl);
        assertThat(getRangesInParallelImpl).hasSameSizeAs(getRangesLazyImpl);

        for (int i = 0; i < getRangesWithPrefetchingImpl.size(); i++) {
            assertThat(BatchingVisitables.copyToList(getRangesWithPrefetchingImpl.get(i)))
                    .hasSize(expectedRangeSize);
            assertThat(BatchingVisitables.copyToList(getRangesInParallelImpl.get(i)))
                    .hasSize(expectedRangeSize);
            assertThat(BatchingVisitables.copyToList(getRangesLazyImpl.get(i))).hasSize(expectedRangeSize);
        }
    }

    private void verifyAllGetRangesImplsNumRanges(
            Transaction tx, Iterable<RangeRequest> rangeRequests, List<String> expectedValues) {
        Iterable<BatchingVisitable<RowResult<byte[]>>> getRangesWithPrefetchingImpl =
                tx.getRanges(TEST_TABLE, rangeRequests);
        Iterable<BatchingVisitable<RowResult<byte[]>>> getRangesInParallelImpl = tx.getRanges(
                        TEST_TABLE, rangeRequests, 2, (rangeRequest, visitable) -> visitable)
                .collect(Collectors.toList());
        Iterable<BatchingVisitable<RowResult<byte[]>>> getRangesLazyImpl =
                tx.getRangesLazy(TEST_TABLE, rangeRequests).collect(Collectors.toList());

        assertThat(extractStringsFromVisitables(getRangesWithPrefetchingImpl))
                .containsExactlyElementsOf(expectedValues);
        assertThat(extractStringsFromVisitables(getRangesInParallelImpl)).containsExactlyElementsOf(expectedValues);
        assertThat(extractStringsFromVisitables(getRangesLazyImpl)).containsExactlyElementsOf(expectedValues);
    }

    private List<String> extractStringsFromVisitables(Iterable<BatchingVisitable<RowResult<byte[]>>> visitables) {
        return BatchingVisitables.concat(visitables)
                .transform(RowResult::getOnlyColumnValue)
                .transform(bytes -> new String(bytes, StandardCharsets.UTF_8))
                .immutableCopy();
    }

    private static byte[] row(int index) {
        return toBytes("row" + index);
    }

    private static byte[] column(int index) {
        return toBytes("col" + index);
    }

    private static byte[] value(int index) {
        return toBytes("value" + index);
    }

    private static byte[] toBytes(String string) {
        return PtBytes.toBytes(string);
    }

    private static void assertMatchingRowAndInColumnOrder(Map.Entry<Cell, byte[]> fst, Map.Entry<Cell, byte[]> snd) {
        assertThat(fst.getKey().getRowName()).isEqualTo(snd.getKey().getRowName());
        assertThat(UnsignedBytes.lexicographicalComparator()
                        .compare(fst.getKey().getColumnName(), snd.getKey().getColumnName()))
                .isLessThan(0);
    }
}

/*
 * Copyright 2018 Palantir Technologies
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

package com.palantir.atlasdb.compact;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;

public class CompactPriorityCalculatorTest {
    private static final String TABLE_1 = "table1";
    private static final String TABLE_2 = "table2";

    private final Transaction mockTx = mock(Transaction.class);
    private final TransactionManager transactionManager = mock(TransactionManager.class);
    private final SweepHistoryProvider sweepHistoryProvider = mock(SweepHistoryProvider.class);
    private final CompactionHistoryProvider compactionHistoryProvider = mock(CompactionHistoryProvider.class);

    private final CompactPriorityCalculator calculator = new CompactPriorityCalculator(transactionManager,
            sweepHistoryProvider, compactionHistoryProvider);

    @Test
    public void returnsEmptyWhenNothingHasBeenSwept() {
        when(sweepHistoryProvider.getHistory(mockTx)).thenReturn(ImmutableMap.of());
        when(compactionHistoryProvider.getHistory(mockTx)).thenReturn(ImmutableMap.of());

        Optional<String> table = calculator.selectTableToCompactInternal(mockTx);
        assertThat(table).isEmpty();
    }

    @Test
    public void returnsUncompactedTableIfPossible() {
        when(sweepHistoryProvider.getHistory(mockTx)).thenReturn(ImmutableMap.of(TABLE_1, 1L, TABLE_2, 2L));
        when(compactionHistoryProvider.getHistory(mockTx)).thenReturn(ImmutableMap.of(TABLE_2, 3L));

        Optional<String> table = calculator.selectTableToCompactInternal(mockTx);
        assertThat(table).isEqualTo(Optional.of(TABLE_1));
    }

    @Test
    public void returnsTableWithBiggestCompactionToSweepTime() {
        // TABLE_1 was compacted more recently, but was also swept more recently, and more time passed between its last
        // compaction and its last sweep - so without other information, we assume there's more to compact.
        when(sweepHistoryProvider.getHistory(mockTx)).thenReturn(ImmutableMap.of(TABLE_1, 7L, TABLE_2, 5L));
        when(compactionHistoryProvider.getHistory(mockTx)).thenReturn(ImmutableMap.of(TABLE_1, 4L, TABLE_2, 3L));

        Optional<String> table = calculator.selectTableToCompactInternal(mockTx);
        assertThat(table).isEqualTo(Optional.of(TABLE_1));
    }

    @Test
    public void canReturnTableEvenIfItWasCompactedAfterTheLastSweep() {
        when(sweepHistoryProvider.getHistory(mockTx)).thenReturn(ImmutableMap.of(TABLE_1, 4L, TABLE_2, 3L));
        when(compactionHistoryProvider.getHistory(mockTx)).thenReturn(ImmutableMap.of(TABLE_1, 7L, TABLE_2, 5L));

        Optional<String> table = calculator.selectTableToCompactInternal(mockTx);
        assertThat(table).isEqualTo(Optional.of(TABLE_2));
    }
}

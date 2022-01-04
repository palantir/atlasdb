/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.cassandra.backup.transaction;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.palantir.timestamp.FullyBoundedTimestampRange;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class TransactionsTableInteractionTest {
    private static final DefaultRetryPolicy POLICY = DefaultRetryPolicy.INSTANCE;
    private static final FullyBoundedTimestampRange FULL_RANGE =
            FullyBoundedTimestampRange.of(Range.closed(1L, 10_000_000L));

    @Test
    public void testGetOnlyTxn1() {
        TransactionsTableInteraction onlyInteraction = getSingleInteraction(1);

        assertThat(onlyInteraction).isExactlyInstanceOf(Transactions1TableInteraction.class);
        assertThat(onlyInteraction.getTimestampRange()).isEqualTo(FULL_RANGE);
    }

    @Test
    public void testGetOnlyTxn2() {
        TransactionsTableInteraction onlyInteraction = getSingleInteraction(2);

        assertThat(onlyInteraction).isExactlyInstanceOf(Transactions2TableInteraction.class);
        assertThat(onlyInteraction.getTimestampRange()).isEqualTo(FULL_RANGE);
    }

    @Test
    public void testGetOnlyTxn3() {
        TransactionsTableInteraction onlyInteraction = getSingleInteraction(3);

        assertThat(onlyInteraction).isExactlyInstanceOf(Transactions3TableInteraction.class);
        assertThat(onlyInteraction.getTimestampRange()).isEqualTo(FULL_RANGE);
    }

    private TransactionsTableInteraction getSingleInteraction(int version) {
        Map<FullyBoundedTimestampRange, Integer> ranges = ImmutableMap.of(FULL_RANGE, version);

        List<TransactionsTableInteraction> transactionTableInteractions =
                TransactionsTableInteraction.getTransactionTableInteractions(ranges, POLICY);

        assertThat(transactionTableInteractions).hasSize(1);
        return Iterables.getOnlyElement(transactionTableInteractions);
    }

    @Test
    public void testGetAllTxnTables() {
        Map<FullyBoundedTimestampRange, Integer> ranges = ImmutableMap.of(
                range(1L, 5L), 1,
                range(6L, 10L), 2,
                range(11L, 15L), 3);

        List<TransactionsTableInteraction> transactionsTableInteractions =
                TransactionsTableInteraction.getTransactionTableInteractions(ranges, POLICY);
        assertThat(transactionsTableInteractions).hasSize(3);

        assertThat(transactionsTableInteractions)
                .hasExactlyElementsOfTypes(
                        Transactions1TableInteraction.class,
                        Transactions2TableInteraction.class,
                        Transactions3TableInteraction.class);
    }

    @Test
    public void testGetsEachTableMultipleTimes() {
        Map<FullyBoundedTimestampRange, Integer> ranges = ImmutableMap.of(
                range(1L, 5L), 1,
                range(6L, 10L), 2,
                range(11L, 15L), 1,
                range(16L, 20L), 2);

        List<TransactionsTableInteraction> transactionsTableInteractions =
                TransactionsTableInteraction.getTransactionTableInteractions(ranges, POLICY);
        assertThat(transactionsTableInteractions).hasSize(4);

        assertThat(transactionsTableInteractions)
                .hasExactlyElementsOfTypes(
                        Transactions1TableInteraction.class,
                        Transactions2TableInteraction.class,
                        Transactions1TableInteraction.class,
                        Transactions2TableInteraction.class);
    }

    private static FullyBoundedTimestampRange range(long lower, long upper) {
        return FullyBoundedTimestampRange.of(Range.closed(lower, upper));
    }
}

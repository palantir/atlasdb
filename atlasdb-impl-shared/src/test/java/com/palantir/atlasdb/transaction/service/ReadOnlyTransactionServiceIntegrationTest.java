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

package com.palantir.atlasdb.transaction.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.timestamp.InMemoryTimestampService;
import org.junit.Test;

public class ReadOnlyTransactionServiceIntegrationTest {
    private static final long COORDINATION_QUANTUM = 100_000_000L;

    private final InMemoryKeyValueService keyValueService = new InMemoryKeyValueService(true);
    private final InMemoryTimestampService timestampService = new InMemoryTimestampService();
    private final TransactionService writeTransactionService
            = TransactionServices.createRaw(keyValueService, timestampService, false);
    private final TransactionService readOnlyTransactionService
            = TransactionServices.createReadOnlyTransactionServiceIgnoresUncommittedTransactionsDoesNotRollBack(
                    keyValueService, MetricsManagers.createForTests());

    @Test
    public void canReadAlreadyAgreedValuesEvenAfterAdditionalCoordinations() {
        writeTransactionService.putUnlessExists(1L, 8L);

        assertThat(readOnlyTransactionService.get(1L)).isEqualTo(8L);
        assertThat(readOnlyTransactionService.get(8L)).isNull();
        assertThat(readOnlyTransactionService.get(COORDINATION_QUANTUM + 1L)).isNull();

        timestampService.fastForwardTimestamp(COORDINATION_QUANTUM);

        writeTransactionService.putUnlessExists(COORDINATION_QUANTUM + 1L, COORDINATION_QUANTUM + 5L);
        assertThat(readOnlyTransactionService.get(COORDINATION_QUANTUM + 1L)).isEqualTo(COORDINATION_QUANTUM + 5L);
        assertThat(readOnlyTransactionService.get(Long.MAX_VALUE)).isNull();
    }

    @Test
    public void canReadMultipleAgreedValuesEvenAfterAdditionalCoordinations() {
        timestampService.fastForwardTimestamp(COORDINATION_QUANTUM);
        writeTransactionService.putUnlessExists(1L, 8L);
        writeTransactionService.putUnlessExists(COORDINATION_QUANTUM + 1L, COORDINATION_QUANTUM + 5L);

        assertThat(readOnlyTransactionService.get(
                ImmutableList.of(1L, COORDINATION_QUANTUM + 1L, 2 * COORDINATION_QUANTUM + 1L)))
                .isEqualTo(ImmutableMap.of(1L, 8L, COORDINATION_QUANTUM + 1L, COORDINATION_QUANTUM + 5L));
    }
}

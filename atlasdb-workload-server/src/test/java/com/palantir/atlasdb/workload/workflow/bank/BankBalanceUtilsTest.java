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

package com.palantir.atlasdb.workload.workflow.bank;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.when;

import java.security.SecureRandom;
import java.util.Map;
import java.util.Optional;
import one.util.streamex.EntryStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public final class BankBalanceUtilsTest {

    private static final Map<Integer, Optional<Integer>> VALID_BALANCES = Map.of(0, Optional.of(1), 1, Optional.of(1));

    @Mock
    private SecureRandom random;

    @Test
    public void validateOrGenerateBalancesReturnsEmptyWhenOnlyOneAccountMissing() {
        assertThat(BankBalanceUtils.validateOrGenerateBalances(Map.of(0, Optional.of(1), 1, Optional.empty()), 2, 1))
                .isEmpty();
    }

    @Test
    public void validateOrGenerateBalancesGeneratesWhenAllEmpty() {
        assertThat(BankBalanceUtils.validateOrGenerateBalances(Map.of(0, Optional.empty(), 1, Optional.empty()), 2, 1))
                .contains(Map.of(0, 1, 1, 1));
    }

    @Test
    public void validateOrGenerateBalancesEmptyWhenSumOfBalancesDoNotMatchTotal() {
        assertThat(BankBalanceUtils.validateOrGenerateBalances(VALID_BALANCES, 2, 3))
                .isEmpty();
    }

    @Test
    public void validateOrGenerateBalancesReturnsBalancesWhenTotalsMatch() {
        assertThat(BankBalanceUtils.validateOrGenerateBalances(VALID_BALANCES, 2, 1))
                .contains(
                        EntryStream.of(VALID_BALANCES).mapValues(Optional::get).toMap());
    }

    @Test
    public void performTransfersReturnsBalancesWithCorrectSum() {
        when(random.nextInt(anyInt())).thenReturn(0, 1);
        Map<Integer, Integer> balances = Map.of(0, 1, 1, 1);
        Map<Integer, Integer> newBalances = BankBalanceUtils.performTransfers(balances, 1, 1, random);
        assertThat(newBalances).containsExactlyInAnyOrderEntriesOf(Map.of(0, 0, 1, 2));
        assertThat(newBalances.values().stream().mapToInt(Integer::intValue).sum())
                .isEqualTo(2);
    }

    @Test
    public void performsTransfersOnlyIfValueWouldNotBeBelowZero() {
        when(random.nextInt(anyInt())).thenReturn(0, 1);
        Map<Integer, Integer> balances = Map.of(0, 0, 1, 2);
        Map<Integer, Integer> newBalances = BankBalanceUtils.performTransfers(balances, 1, 1, random);
        assertThat(newBalances).containsExactlyInAnyOrderEntriesOf(balances);
    }
}

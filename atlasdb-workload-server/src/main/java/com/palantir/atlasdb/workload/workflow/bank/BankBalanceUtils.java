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

import com.google.common.collect.ImmutableMap;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import one.util.streamex.EntryStream;

public final class BankBalanceUtils {

    private BankBalanceUtils() {}

    /**
     * Checks the provided balances that all are present, and if so, returns them.
     * If all balances are empty, it means we need to initialize, so we generate them.
     *
     * Otherwise, return an empty map to indicate that we are in a faulty state.
     */
    public static Optional<Map<Integer, Integer>> validateOrGenerateBalances(
            Map<Integer, Optional<Integer>> maybeBalances, Integer numberOfAccounts, Integer balancePerAccount) {
        if (allValuesPresent(maybeBalances)) {
            Integer total = numberOfAccounts * balancePerAccount;
            Map<Integer, Integer> balances =
                    EntryStream.of(maybeBalances).mapValues(Optional::get).toMap();
            if (!sumOfBalancesMatchTotal(balances, total)) {
                return Optional.empty();
            }
            return Optional.of(balances);
        }

        if (allValuesEmpty(maybeBalances)) {
            return Optional.of(generateBalances(numberOfAccounts, balancePerAccount));
        }

        return Optional.empty();
    }

    public static Map<Integer, Integer> performTransfers(
            Map<Integer, Integer> balances, int numberOfTransfers, int transferAmount, SecureRandom random) {
        Map<Integer, Integer> newBalances = new HashMap<>(balances);
        for (int idx = 0; idx < numberOfTransfers; idx++) {
            int from = random.nextInt(newBalances.size());
            int to = random.nextInt(newBalances.size());

            if (newBalances.get(from) - transferAmount < 0) {
                continue;
            }

            newBalances.compute(from, (k, v) -> v - transferAmount);
            newBalances.compute(to, (k, v) -> v + transferAmount);
        }
        return ImmutableMap.copyOf(newBalances);
    }

    private static boolean sumOfBalancesMatchTotal(Map<Integer, Integer> balances, Integer totalBalance) {
        return balances.values().stream().mapToInt(Integer::intValue).sum() == totalBalance;
    }

    private static boolean allValuesPresent(Map<Integer, Optional<Integer>> balances) {
        return balances.values().stream().allMatch(Optional::isPresent);
    }

    private static boolean allValuesEmpty(Map<Integer, Optional<Integer>> balances) {
        return balances.values().stream().allMatch(Optional::isEmpty);
    }

    private static Map<Integer, Integer> generateBalances(Integer numberOfAccounts, Integer balancePerAccount) {
        return IntStream.range(0, numberOfAccounts)
                .boxed()
                .collect(Collectors.toMap(Function.identity(), index -> balancePerAccount));
    }
}

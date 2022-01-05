/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.config;

import com.google.common.collect.ImmutableList;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.immutables.value.Value;

public class SocialDistancing {
    public static void main(String[] args) {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        List<String> inputs = br.lines().collect(Collectors.toList());
        int maxVisitors = Integer.parseInt(inputs.get(0));
        List<Household> households = inputs.subList(1, inputs.size()).stream().map(Household::fromString)
                .collect(Collectors.toList());
        Map<Integer, List<Household>> householdsBySize =
                households.stream().collect(Collectors.groupingBy(Household::size));
        Set<OrderedHouseholdPair> orderedPairs = new HashSet<>();

        for (int index = 0; index < households.size(); index++) {
            for (int secondIndex = index + 1; secondIndex < households.size(); secondIndex++) {
                OrderedHouseholdPair pair = OrderedHouseholdPair.of(households.get(index), households.get(secondIndex));
                if (pair.first().size() > maxVisitors && pair.second().size() > maxVisitors) {
                    // Impossible
                    throw new IllegalStateException("Not possible for everyone to visit everyone; two households are"
                            + " greater than the visitation limit: " + pair);
                }
                orderedPairs.add(pair);
            }
        }

        // possibility 1: random solution
        List<Visit> random = randomAnswer(orderedPairs, maxVisitors);

        // possibility 2: random restart hill climbing / simulated annealing style thing

        // possibility 3: memoisation

        System.out.println(random);
    }

    private static List<Visit> randomAnswer(Set<OrderedHouseholdPair> orderedPairs, int maxVisitors) {
        RandomElementRemovableSet<OrderedHouseholdPair> remainingPairs = RandomElementRemovableSet.copyOf(orderedPairs);
        while (!remainingPairs.isEmpty()) {
            // Come up with a schedule for the day
            OrderedHouseholdPair mandate = remainingPairs.getRandom();
            
        }
        return null;
    }

    static class RandomElementRemovableSet<T> {
        private final List<T> backingList = new ArrayList<>();
        private final Map<T, Integer> elementToIndex = new HashMap<>();

        public static <T> RandomElementRemovableSet<T> copyOf(Set<T> base) {
            RandomElementRemovableSet<T> result = new RandomElementRemovableSet<>();
            base.forEach(result::add);
            return result;
        }

        public boolean isEmpty() {
            return elementToIndex.isEmpty();
        }

        public boolean add(T element) {
            if (elementToIndex.containsKey(element)) {
                return false;
            }
            elementToIndex.put(element, backingList.size());
            backingList.add(element);
            return true;
        }

        public boolean remove(T element) {
            Integer targetIndex = elementToIndex.get(element);
            if (targetIndex == null) {
                return false;
            }
            T lastElement = backingList.get(backingList.size() - 1);
            backingList.set(targetIndex, lastElement);
            backingList.remove(backingList.size() - 1);
            elementToIndex.put(lastElement, targetIndex);
            elementToIndex.remove(element);
            return true;
        }

        public T getRandom() {
            return backingList.get(ThreadLocalRandom.current().nextInt(0, backingList.size()));
        }

        public Stream<T> getRemainingElements() {
            return ImmutableList.copyOf(backingList);
        }
    }

    @Value.Immutable
    interface OrderedHouseholdPair {
        Comparator<Household> HOUSEHOLD_COMPARATOR = Comparator.comparing(Household::name)
                .thenComparingInt(Household::size);

        @Value.Parameter
        Household first();

        @Value.Parameter
        Household second();

        static OrderedHouseholdPair of(Household first, Household second) {
            if (HOUSEHOLD_COMPARATOR.compare(first, second) > 0) {
                return ImmutableOrderedHouseholdPair.of(second, first);
            }
            return ImmutableOrderedHouseholdPair.of(first, second);
        }
    }

    @Value.Immutable
    interface Visit {
        int day();
        Household host();
        List<Household> guests();

        default String getPrintableString() {
            return String.format(
                    "Day %d. Host: %s. Guests: %s.",
                    day(),
                    host(),
                    guests());
        }
    }

    @Value.Immutable
    interface Household {
        String name();
        int size();

        // Converts the string "(Abraham, 3)" to the correct household object
        static Household fromString(String input) {
            String noBrackets = input.substring(1, input.length() - 1);
            String[] commaSplit = noBrackets.split(",");
            return ImmutableHousehold.builder()
                    .name(commaSplit[0].trim())
                    .size(Integer.parseInt(commaSplit[1].trim()))
                    .build();
        }
    }
}

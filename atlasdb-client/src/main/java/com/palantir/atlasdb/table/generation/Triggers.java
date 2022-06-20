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
package com.palantir.atlasdb.table.generation;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.transaction.api.Transaction;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Triggers {
    public static <T, U extends T> List<T> getAllTriggers(
            Transaction t, List<Function<? super Transaction, U>> sharedTriggers, T[] triggers) {
        int totalTriggers = sharedTriggers.size() + triggers.length;
        if (totalTriggers == 0) {
            // common case is there are no shared or transaction specific triggers
            return ImmutableList.of();
        }

        Stream<U> sharedTriggerStream = sharedTriggers.stream().map(trigger -> trigger.apply(t));
        if (triggers.length == 0) {
            return sharedTriggerStream.collect(Collectors.toList());
        }
        return Stream.concat(Arrays.stream(triggers), sharedTriggerStream).collect(Collectors.toList());
    }
}

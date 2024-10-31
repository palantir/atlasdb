/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock;

import static com.palantir.logsafe.testing.Assertions.assertThatLoggableExceptionThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import com.google.common.collect.Iterators;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public final class LimitingLongSupplierTest {
    @ValueSource(ints = {1, 2, 10, 100, 981, 999})
    @ParameterizedTest
    public void returnsTimestampsFromDelegateIfLimitNotExceededAndThrowsOnceExceeded(int limit) {
        List<Long> longs = createRandomListOfSize(limit);
        LongSupplier supplier = new LimitingLongSupplier(createCyclingSupplierFromList(longs), limit);

        List<Long> grabbedUpToLimit =
                Stream.generate(supplier::getAsLong).limit(limit).collect(Collectors.toList());
        assertThat(grabbedUpToLimit).containsExactlyElementsOf(longs);

        // confirm that all calls after the limit is reached throw
        for (int i = 0; i < 5; i++) {
            assertThatLoggableExceptionThrownBy(supplier::getAsLong)
                    .isInstanceOf(SafeRuntimeException.class)
                    .hasExactlyArgs(SafeArg.of("limit", limit), SafeArg.of("fulfilled", limit));
        }
    }

    @Test
    public void propagatesDelegateException() {
        RuntimeException exception = new RuntimeException("delegate exception");
        LongSupplier throwingSupplier = () -> {
            throw exception;
        };
        LongSupplier supplier = new LimitingLongSupplier(throwingSupplier, 1);

        assertThatThrownBy(supplier::getAsLong).isEqualTo(exception);
    }

    private static List<Long> createRandomListOfSize(int limit) {
        return ThreadLocalRandom.current().longs().limit(limit).boxed().collect(Collectors.toList());
    }

    private static LongSupplier createCyclingSupplierFromList(List<Long> longs) {
        Iterator<Long> iterator = Iterators.cycle(longs);
        return iterator::next;
    }
}

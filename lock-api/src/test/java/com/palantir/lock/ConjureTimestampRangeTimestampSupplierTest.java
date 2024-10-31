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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatCode;

import com.palantir.atlasdb.timelock.api.ConjureTimestampRange;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public final class ConjureTimestampRangeTimestampSupplierTest {

    @MethodSource("createStartAndSizeForRangeCreation")
    @ParameterizedTest
    public void throwsOnlyWhenAttemptToGrabMoreTimestampsThanInRangeIsMade(long start, int size) {
        ConjureTimestampRange range = ConjureTimestampRange.of(start, size);
        ConjureTimestampRangeTimestampSupplier supplier = new ConjureTimestampRangeTimestampSupplier(range);

        assertThatCode(() -> {
                    for (int i = 0; i < size; i++) {
                        supplier.getAsLong();
                    }
                })
                .doesNotThrowAnyException();

        // fails on all invocations after it is exhausted
        for (int i = 0; i < 5; i++) {
            assertThatLoggableExceptionThrownBy(supplier::getAsLong)
                    .isInstanceOf(SafeRuntimeException.class)
                    .hasExactlyArgs(SafeArg.of("range", range));
        }
    }

    @MethodSource("createStartAndSizeForRangeCreation")
    @ParameterizedTest
    public void supplierReturnsLongsFromRange(long start, int size) {
        ConjureTimestampRange range = ConjureTimestampRange.of(start, size);
        ConjureTimestampRangeTimestampSupplier supplier = new ConjureTimestampRangeTimestampSupplier(range);

        List<Long> expected = LongStream.range(start, start + size).boxed().collect(Collectors.toList());

        List<Long> fetched = Stream.generate(supplier::getAsLong).limit(size).collect(Collectors.toList());

        assertThat(fetched).containsExactlyElementsOf(expected);
    }

    private static Set<Arguments> createStartAndSizeForRangeCreation() {
        return Set.of(
                Arguments.arguments(1L, 10),
                Arguments.arguments(5L, 1),
                Arguments.arguments(Integer.MAX_VALUE + 5L, 10000),
                Arguments.arguments(1000, 123));
    }
}

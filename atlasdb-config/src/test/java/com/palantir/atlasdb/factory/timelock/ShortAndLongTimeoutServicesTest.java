/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.factory.timelock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.math.IntMath;
import com.palantir.atlasdb.factory.ServiceCreator;
import org.junit.Test;

public class ShortAndLongTimeoutServicesTest {
    @Test
    public void createInvokesCorrectMethodsOnServiceCreator() {
        Object shortTimeout = new Object();
        Object longTimeout = new Object();

        ServiceCreator serviceCreator = mock(ServiceCreator.class);
        when(serviceCreator.createServiceWithShortTimeout(any())).thenReturn(shortTimeout);
        when(serviceCreator.createService(any())).thenReturn(longTimeout);

        ShortAndLongTimeoutServices<Object> services = ShortAndLongTimeoutServices.create(serviceCreator, Object.class);
        assertThat(services.shortTimeout()).isEqualTo(shortTimeout);
        assertThat(services.longTimeout()).isEqualTo(longTimeout);

        verify(serviceCreator).createService(Object.class);
        verify(serviceCreator).createServiceWithShortTimeout(Object.class);
        verifyNoMoreInteractions(serviceCreator);
    }

    @Test
    public void mapOperatesOnCorrespondingElements() {
        ShortAndLongTimeoutServices<Integer> integers = ImmutableShortAndLongTimeoutServices.<Integer>builder()
                .shortTimeout(1)
                .longTimeout(3)
                .build();

        ShortAndLongTimeoutServices<String> strings = integers.map(Object::toString);
        assertThat(strings.shortTimeout()).isEqualTo("1");
        assertThat(strings.longTimeout()).isEqualTo("3");
    }

    @Test
    public void zipWithZipsCorrectElements() {
        ShortAndLongTimeoutServices<Integer> numbers = ImmutableShortAndLongTimeoutServices.<Integer>builder()
                .shortTimeout(1)
                .longTimeout(10)
                .build();
        ShortAndLongTimeoutServices<String> stringNumbers = ImmutableShortAndLongTimeoutServices.<String>builder()
                .shortTimeout("100")
                .longTimeout("1000")
                .build();

        ShortAndLongTimeoutServices<Integer> zipped = numbers.zipWith(stringNumbers,
                (integer, string) -> integer + Integer.parseInt(string));
        assertThat(zipped.shortTimeout()).isEqualTo(101);
        assertThat(zipped.longTimeout()).isEqualTo(1010);
    }

    @Test
    public void zipWithUsesZippedObjectAsFirstArgument() {
        ShortAndLongTimeoutServices<Integer> minuends = ImmutableShortAndLongTimeoutServices.<Integer>builder()
                .shortTimeout(100)
                .longTimeout(100)
                .build();
        ShortAndLongTimeoutServices<Integer> subtrahends = ImmutableShortAndLongTimeoutServices.<Integer>builder()
                .shortTimeout(1)
                .longTimeout(1)
                .build();

        ShortAndLongTimeoutServices<Integer> zipped = minuends.zipWith(subtrahends, IntMath::checkedSubtract);
        assertThat(zipped.shortTimeout()).isEqualTo(99);
        assertThat(zipped.longTimeout()).isEqualTo(99);

    }
}

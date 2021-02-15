/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("unchecked") // Mock usage
public class CachedTransformingSupplierTest {
    private final Supplier<String> STRING_SUPPLIER = mock(Supplier.class);
    private final Function<String, Long> LONG_FUNCTION = mock(Function.class);
    private final CachedTransformingSupplier<String, Long> TRANSFORMING_SUPPLIER =
            new CachedTransformingSupplier<>(STRING_SUPPLIER, LONG_FUNCTION);

    @Before
    public void setUp() {
        when(LONG_FUNCTION.apply(anyString())).thenAnswer(invocation -> Long.parseLong(invocation.getArgument(0)));
    }

    @Test
    public void getsFreshValuesFromSupplier() {
        when(STRING_SUPPLIER.get()).thenReturn("42").thenReturn("4242");
        assertThat(TRANSFORMING_SUPPLIER.get()).isEqualTo(42);
        assertThat(TRANSFORMING_SUPPLIER.get()).isEqualTo(4242);
    }

    @Test
    public void onlyCallsTransformOnceIfUnderlyingValueNotChanged() {
        when(STRING_SUPPLIER.get()).thenReturn("42");
        IntStream.range(0, 10)
                .forEach($ -> assertThat(TRANSFORMING_SUPPLIER.get()).isEqualTo(42));
        verify(STRING_SUPPLIER, times(10)).get();
        verify(LONG_FUNCTION, times(1)).apply(anyString());
    }

    @Test
    public void throwsIfSupplierThrows() {
        RuntimeException ex = new RuntimeException();
        when(STRING_SUPPLIER.get()).thenThrow(ex);
        assertThatThrownBy(TRANSFORMING_SUPPLIER::get).isEqualTo(ex);
    }

    @Test
    public void throwsIfMappingFunctionThrows() {
        when(STRING_SUPPLIER.get()).thenReturn("42!)*()");
        assertThatThrownBy(TRANSFORMING_SUPPLIER::get).isInstanceOf(NumberFormatException.class);
    }
}

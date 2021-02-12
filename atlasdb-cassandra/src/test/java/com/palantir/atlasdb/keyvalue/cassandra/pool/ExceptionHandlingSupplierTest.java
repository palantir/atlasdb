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

package com.palantir.atlasdb.keyvalue.cassandra.pool;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.function.Supplier;
import org.junit.Test;

public final class ExceptionHandlingSupplierTest {
    private static final String VALUE = "value";

    private Supplier<String> supplier = mock(Supplier.class);

    @Test
    public void supplierCatchesExceptions() {
        when(supplier.get()).thenThrow(new RuntimeException()).thenReturn(VALUE);
        Supplier<Optional<String>> exceptionHandlingSupplier = ExceptionHandlingSupplier.create(supplier, 5);

        assertThat(exceptionHandlingSupplier.get()).isEmpty();
        assertThat(exceptionHandlingSupplier.get()).hasValue(VALUE);

        verify(supplier, times(2)).get();
    }

    @Test
    public void underlyingSupplierNoLongerCalledAfterExceptions() {
        when(supplier.get())
                .thenThrow(new RuntimeException())
                .thenThrow(new RuntimeException())
                .thenReturn(VALUE);
        Supplier<Optional<String>> exceptionHandlingSupplier = ExceptionHandlingSupplier.create(supplier, 1);

        assertThat(exceptionHandlingSupplier.get()).isEmpty();
        assertThat(exceptionHandlingSupplier.get()).isEmpty();
        assertThat(exceptionHandlingSupplier.get()).isEmpty();

        verify(supplier, times(2)).get();
    }

    @Test
    public void successfulSupplierCallsResetExceptionCounter() {
        when(supplier.get())
                .thenThrow(new RuntimeException())
                .thenReturn(VALUE)
                .thenThrow(new RuntimeException())
                .thenReturn(VALUE);
        Supplier<Optional<String>> exceptionHandlingSupplier = ExceptionHandlingSupplier.create(supplier, 2);

        assertThat(exceptionHandlingSupplier.get()).isEmpty();
        assertThat(exceptionHandlingSupplier.get()).hasValue(VALUE);
        assertThat(exceptionHandlingSupplier.get()).isEmpty();
        assertThat(exceptionHandlingSupplier.get()).hasValue(VALUE);

        verify(supplier, times(4)).get();
    }
}

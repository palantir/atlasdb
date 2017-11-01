/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.http;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.junit.Test;

public class RecreatingInvocationHandlerTest {
    private static final Long FORTY_TWO = 42L;

    @Test
    public void propagatesRepeatedChangesThroughDeltaSupplier() {
        AtomicLong atomicLong = new AtomicLong();
        Supplier<Optional<Long>> deltaSupplier =
                RecreatingInvocationHandler.wrapInDeltaSupplier(atomicLong::incrementAndGet);

        assertThat(deltaSupplier.get()).isPresent().contains(1L);
        assertThat(deltaSupplier.get()).isPresent().contains(2L);
        assertThat(deltaSupplier.get()).isPresent().contains(3L);
    }

    @Test
    public void deltaSupplierReturnsEmptyIfUnderlyingValueHasNotChanged() {
        AtomicLong atomicLong = new AtomicLong(FORTY_TWO);
        Supplier<Optional<Long>> deltaSupplier =
                RecreatingInvocationHandler.wrapInDeltaSupplier(atomicLong::get);

        assertThat(deltaSupplier.get()).isPresent().contains(FORTY_TWO);
        assertThat(deltaSupplier.get()).isEmpty();
        assertThat(deltaSupplier.get()).isEmpty();
    }

    @Test
    public void deltaSupplierCanReturnNewValuesAfterReturningEmptyIfSupplierChanges() {
        AtomicLong atomicLong = new AtomicLong(FORTY_TWO);
        Supplier<Optional<Long>> deltaSupplier =
                RecreatingInvocationHandler.wrapInDeltaSupplier(atomicLong::get);

        assertThat(deltaSupplier.get()).isPresent().contains(FORTY_TWO);
        assertThat(deltaSupplier.get()).isEmpty();

        atomicLong.incrementAndGet();

        assertThat(deltaSupplier.get()).isPresent().contains(FORTY_TWO + 1L);
        assertThat(deltaSupplier.get()).isEmpty();
    }

    @Test
    public void deltaSupplierSkipsMultipleChanges() {
        AtomicLong atomicLong = new AtomicLong(FORTY_TWO);
        Supplier<Optional<Long>> deltaSupplier =
                RecreatingInvocationHandler.wrapInDeltaSupplier(atomicLong::get);

        atomicLong.incrementAndGet();
        atomicLong.incrementAndGet();
        atomicLong.incrementAndGet();

        assertThat(deltaSupplier.get()).isPresent().contains(FORTY_TWO + 3L);
    }

}

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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.immutables.value.Value;
import org.junit.Test;

public class RecreatingInvocationHandlerTest {
    private static final Long FORTY_TWO = 42L;

    @Test
    public void deltaSupplierReturnsValuesSupplied() {
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
    public void deltaSupplierReturnsNewValuesAfterReturningEmptyIfSupplierChanges() {
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
    public void deltaSupplierSkipsIntermediateChanges() {
        AtomicLong atomicLong = new AtomicLong(FORTY_TWO);
        Supplier<Optional<Long>> deltaSupplier =
                RecreatingInvocationHandler.wrapInDeltaSupplier(atomicLong::get);

        atomicLong.incrementAndGet();
        atomicLong.incrementAndGet();
        atomicLong.incrementAndGet();

        assertThat(deltaSupplier.get()).isPresent().contains(FORTY_TWO + 3L);
    }

    @Test
    public void createsNewDelegateIfSupplierReturnsPresent() {
        Supplier<Optional<Object>> supplier = () -> Optional.of(new Object());
        Identifiable identifiable = RecreatingInvocationHandler.createWithRawDeltaSupplier(supplier,
                unused -> ImmutableIdentifiable.of(UUID.randomUUID()),
                Identifiable.class);

        UUID firstUuid = identifiable.uuid();
        UUID secondUuid = identifiable.uuid();

        assertThat(firstUuid).isNotEqualTo(secondUuid);
    }

    @Test
    public void throwsIfSupplierInitiallyReturnsEmpty() {
        Supplier<Optional<Object>> supplier = Optional::empty;
        assertThatThrownBy(
                () -> RecreatingInvocationHandler.createWithRawDeltaSupplier(supplier, unused -> "", String.class))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void doesNotCreateNewDelegateIfSupplierReturnsEmpty() {
        AtomicReference<Object> objectRef = new AtomicReference<>(new Object());
        Supplier<Optional<Object>> supplier = () -> Optional.ofNullable(objectRef.get());

        Identifiable identifiable = RecreatingInvocationHandler.createWithRawDeltaSupplier(supplier,
                unused -> ImmutableIdentifiable.of(UUID.randomUUID()),
                Identifiable.class);

        UUID firstUuid = identifiable.uuid();
        objectRef.set(null);
        assertThat(identifiable.uuid()).isEqualTo(firstUuid);
    }

    @Test
    public void canCreateNewDelegateAfterAnEmptyValue() {
        AtomicReference<Object> objectRef = new AtomicReference<>(new Object());
        Supplier<Optional<Object>> supplier = () -> Optional.ofNullable(objectRef.get());

        Identifiable identifiable = RecreatingInvocationHandler.createWithRawDeltaSupplier(supplier,
                unused -> ImmutableIdentifiable.of(UUID.randomUUID()),
                Identifiable.class);

        UUID firstUuid = identifiable.uuid();
        objectRef.set(null);
        assertThat(identifiable.uuid()).isEqualTo(firstUuid);

        objectRef.set(new Object());
        assertThat(identifiable.uuid()).isNotEqualTo(firstUuid);
    }

    @Value.Immutable
    interface Identifiable {
        @Value.Parameter
        UUID uuid();
    }
}

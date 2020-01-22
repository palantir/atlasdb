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
package com.palantir.util;

import static com.palantir.util.AggregatingVersionedSupplier.UNINITIALIZED_VERSION;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Comparator;
import org.junit.Before;
import org.junit.Test;

public class AggregatingVersionedSupplierTest {
    private static final long REFRESH_MILLIS = 1L;

    private AggregatingVersionedSupplier<Long> supplier;

    @Before
    public void setup() {
        supplier = new AggregatingVersionedSupplier<>(
                col -> col.stream().max(Comparator.naturalOrder()).orElse(0L), REFRESH_MILLIS);
    }

    @Test
    public void canUpdateForNewKeys() throws InterruptedException {
        assertThat(supplier.get()).isEqualTo(VersionedType.of(0L, UNINITIALIZED_VERSION + 1L));

        supplier.update(1, 1L);
        waitForUpdate();
        assertThat(supplier.get()).isEqualTo(VersionedType.of(1L, UNINITIALIZED_VERSION + 2L));

        supplier.update(2, 100L);
        waitForUpdate();
        assertThat(supplier.get()).isEqualTo(VersionedType.of(100L, UNINITIALIZED_VERSION + 3L));
    }

    @Test
    public void canUpdateForExistingKeys() throws InterruptedException {
        assertThat(supplier.get()).isEqualTo(VersionedType.of(0L, UNINITIALIZED_VERSION + 1L));

        supplier.update(2, 100L);
        waitForUpdate();
        assertThat(supplier.get()).isEqualTo(VersionedType.of(100L, UNINITIALIZED_VERSION + 2L));

        supplier.update(2, 10L);
        waitForUpdate();
        assertThat(supplier.get()).isEqualTo(VersionedType.of(10L, UNINITIALIZED_VERSION + 3L));
    }

    @Test
    public void versionUpdatesForGetAfterRefreshMillis() throws InterruptedException {
        assertThat(supplier.get()).isEqualTo(VersionedType.of(0L, UNINITIALIZED_VERSION + 1L));

        waitForUpdate();
        assertThat(supplier.get()).isEqualTo(VersionedType.of(0L, UNINITIALIZED_VERSION + 2L));

        waitForUpdate();
        assertThat(supplier.get()).isEqualTo(VersionedType.of(0L, UNINITIALIZED_VERSION + 3L));
    }

    @Test
    public void versionDoesNotUpdateUntilGetIsCalled() throws InterruptedException {
        assertThat(supplier.get()).isEqualTo(VersionedType.of(0L, UNINITIALIZED_VERSION + 1L));

        waitForUpdate();
        waitForUpdate();
        waitForUpdate();
        assertThat(supplier.get()).isEqualTo(VersionedType.of(0L, UNINITIALIZED_VERSION + 2L));
    }

    @Test
    public void suppliedValueDoesNotChangeIfRefreshTimeHasNotPassed() {
        supplier = new AggregatingVersionedSupplier<>(
                col -> col.stream().max(Comparator.naturalOrder()).orElse(0L), 1_000_000);

        assertThat(supplier.get()).isEqualTo(VersionedType.of(0L, UNINITIALIZED_VERSION + 1L));

        supplier.update(1, 1L);
        assertThat(supplier.get()).isEqualTo(VersionedType.of(0L, UNINITIALIZED_VERSION + 1L));
    }

    private void waitForUpdate() throws InterruptedException {
        Thread.sleep(REFRESH_MILLIS + 1);
    }
}

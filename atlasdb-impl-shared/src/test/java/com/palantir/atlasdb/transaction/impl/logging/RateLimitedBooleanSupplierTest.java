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
package com.palantir.atlasdb.transaction.impl.logging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.junit.Test;

import com.google.common.util.concurrent.Uninterruptibles;

@SuppressWarnings("ReturnValueIgnored") // We just care about whether the supplier is invoked, not its return value.
public class RateLimitedBooleanSupplierTest {
    @Test
    public void firstReturnedValueShouldBeTrue() {
        BooleanSupplier booleanSupplier = RateLimitedBooleanSupplier.create(TimeUnit.DAYS.toSeconds(1));
        assertThat(booleanSupplier.getAsBoolean()).isTrue();
    }

    @Test
    public void shouldReturnFalseIfTimeHasNotElapsed() {
        BooleanSupplier booleanSupplier = RateLimitedBooleanSupplier.create(TimeUnit.DAYS.toSeconds(1));
        booleanSupplier.getAsBoolean();
        assertThat(booleanSupplier.getAsBoolean()).isFalse();
    }

    @Test
    public void shouldReturnTrueOnceTimeHasElapsed() {
        BooleanSupplier booleanSupplier = RateLimitedBooleanSupplier.create(TimeUnit.SECONDS.toSeconds(1));
        booleanSupplier.getAsBoolean();
        Uninterruptibles.sleepUninterruptibly(1_500, TimeUnit.MILLISECONDS);
        assertThat(booleanSupplier.getAsBoolean()).isTrue();
    }

    @Test
    public void shouldAlwaysReturnTrueIfCreatedWithNoLimit() {
        BooleanSupplier booleanSupplier = RateLimitedBooleanSupplier.create(0);
        for (int i = 0; i < 100; i++) {
            assertThat(booleanSupplier.getAsBoolean()).isTrue();
        }
    }

    @Test
    public void shouldThrowIfCreatedWithNegativeLimit() {
        assertThatThrownBy(() -> RateLimitedBooleanSupplier.create(-5)).isInstanceOf(IllegalArgumentException.class);
    }
}

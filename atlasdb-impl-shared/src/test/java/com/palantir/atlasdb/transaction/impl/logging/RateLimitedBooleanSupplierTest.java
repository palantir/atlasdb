/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl.logging;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.junit.Test;

import com.google.common.util.concurrent.Uninterruptibles;

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
}

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
package com.palantir.atlasdb.sweep;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.function.LongSupplier;

import org.junit.Test;

public class SweepTimestampTest {
    private final LongSupplier mockImmutableTimestampSupplier = mock(LongSupplier.class);
    private final LongSupplier mockUnreadableTimestampSupplier = mock(LongSupplier.class);

    @Test
    public void thoroughWillReturnTheImmutableTimestamp() {
        when(mockImmutableTimestampSupplier.getAsLong()).thenReturn(123L);

        assertThat(Sweeper.THOROUGH.getSweepTimestampSupplier().getSweepTimestamp(
                mockUnreadableTimestampSupplier, mockImmutableTimestampSupplier)).isEqualTo(123L);
    }

    @Test
    public void conservativeWillReturnTheImmutableTimestampIfItIsLowerThanUnreadableTimestamp() {
        when(mockImmutableTimestampSupplier.getAsLong()).thenReturn(100L);
        when(mockUnreadableTimestampSupplier.getAsLong()).thenReturn(200L);

        assertThat(Sweeper.CONSERVATIVE.getSweepTimestampSupplier().getSweepTimestamp(
                mockUnreadableTimestampSupplier, mockImmutableTimestampSupplier)).isEqualTo(100L);
    }

    @Test
    public void conservativeWillReturnTheUnreadableTimestampIfItIsLowerThanImmutableTimestamp() {
        when(mockImmutableTimestampSupplier.getAsLong()).thenReturn(200L);
        when(mockUnreadableTimestampSupplier.getAsLong()).thenReturn(100L);

        assertThat(Sweeper.CONSERVATIVE.getSweepTimestampSupplier().getSweepTimestamp(
                mockUnreadableTimestampSupplier, mockImmutableTimestampSupplier)).isEqualTo(100L);
    }

}

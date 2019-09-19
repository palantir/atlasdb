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
package com.palantir.atlasdb.sweep;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.palantir.atlasdb.sweep.queue.SpecialTimestampsSupplier;
import java.util.function.LongSupplier;
import org.junit.Test;

public class SweepTimestampTest {
    private final LongSupplier mockImmutableTimestampSupplier = mock(LongSupplier.class);
    private final LongSupplier mockUnreadableTimestampSupplier = mock(LongSupplier.class);
    private final SpecialTimestampsSupplier specialTimestampsSupplier = new SpecialTimestampsSupplier(
            mockUnreadableTimestampSupplier, mockImmutableTimestampSupplier);

    @Test
    public void thoroughWillReturnTheImmutableTimestamp() {
        when(mockImmutableTimestampSupplier.getAsLong()).thenReturn(123L);

        assertThat(Sweeper.THOROUGH.getSweepTimestamp(specialTimestampsSupplier)).isEqualTo(123L);
    }

    @Test
    public void conservativeWillReturnTheImmutableTimestampIfItIsLowerThanUnreadableTimestamp() {
        when(mockImmutableTimestampSupplier.getAsLong()).thenReturn(100L);
        when(mockUnreadableTimestampSupplier.getAsLong()).thenReturn(200L);

        assertThat(Sweeper.CONSERVATIVE.getSweepTimestamp(specialTimestampsSupplier)).isEqualTo(100L);
    }

    @Test
    public void conservativeWillReturnTheUnreadableTimestampIfItIsLowerThanImmutableTimestamp() {
        when(mockImmutableTimestampSupplier.getAsLong()).thenReturn(200L);
        when(mockUnreadableTimestampSupplier.getAsLong()).thenReturn(100L);

        assertThat(Sweeper.CONSERVATIVE.getSweepTimestamp(specialTimestampsSupplier)).isEqualTo(100L);
    }
}

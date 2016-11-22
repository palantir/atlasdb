/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.timestamp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Before;
import org.junit.Test;

public class InMemoryTimestampServiceTest {
    private static final long BOUNDARY_1 = 1000000;
    private static final long BOUNDARY_2 = 5000000;

    private InMemoryTimestampService inMemoryTimestampService;

    @Before
    public void setUp() {
        inMemoryTimestampService = new InMemoryTimestampService();
    }

    @Test
    public void returnsIncreasingTimestamps() {
        long ts1 = inMemoryTimestampService.getFreshTimestamp();
        long ts2 = inMemoryTimestampService.getFreshTimestamp();
        assertThat(ts1).isLessThan(ts2);
    }

    @Test
    public void returnsTimestampRange() {
        int timestampsToGet = 1000;
        TimestampRange range = inMemoryTimestampService.getFreshTimestamps(timestampsToGet);
        assertThat(range.size()).isEqualTo(timestampsToGet);
    }

    @Test
    public void throwsIfRequestingNegativeNumberOfTimestamps() {
        assertThatThrownBy(() -> inMemoryTimestampService.getFreshTimestamps(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void canFastForwardTimestamps() {
        inMemoryTimestampService.fastForwardTimestamp(BOUNDARY_1);
        assertThat(inMemoryTimestampService.getFreshTimestamp()).isGreaterThan(BOUNDARY_1);
    }

    @Test
    public void fastForwardToThePastIsANoOp() {
        inMemoryTimestampService.fastForwardTimestamp(BOUNDARY_2);
        long ts1 = inMemoryTimestampService.getFreshTimestamp();
        inMemoryTimestampService.fastForwardTimestamp(BOUNDARY_1);
        long ts2 = inMemoryTimestampService.getFreshTimestamp();
        assertThat(ts2).isGreaterThan(BOUNDARY_2).isGreaterThan(ts1);
    }

    @Test
    public void throwsAfterInvalidation() {
        inMemoryTimestampService.invalidateTimestamps();
        assertThatThrownBy(inMemoryTimestampService::getFreshTimestamp).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void canBeRevalidatedByFastForward() {
        inMemoryTimestampService.invalidateTimestamps();
        inMemoryTimestampService.fastForwardTimestamp(BOUNDARY_1);
        assertThat(inMemoryTimestampService.getFreshTimestamp()).isGreaterThan(BOUNDARY_1);
    }
}

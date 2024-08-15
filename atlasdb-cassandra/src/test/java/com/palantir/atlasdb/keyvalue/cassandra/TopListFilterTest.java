/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

public final class TopListFilterTest {
    @Test
    public void doesNotFilterWhenCapacityIsNotExhaustedBeforeNextReset() {
        // When the last reset resulted in a partially filled top list, any value coming until the next
        // reset is considered to be in the top list
        TopListFilter<String> topListFilter = new TopListFilter<>(3, Duration.ofMinutes(10));
        assertThat(topListFilter.updateAndGetStatus("key1", 1)).isTrue();
        assertThat(topListFilter.updateAndGetStatus("key2", 2)).isTrue();
        assertThat(topListFilter.updateAndGetStatus("key3", 3)).isTrue();
        assertThat(topListFilter.updateAndGetStatus("key4", 4)).isTrue();
        assertThat(topListFilter.updateAndGetStatus("key5", 5)).isTrue();
    }

    @Test
    public void eventuallyFiltersNewSmallerValueWhenCapacityIsExhausted() {
        TopListFilter<String> topListFilter = new TopListFilter<>(3, Duration.ofNanos(1));
        topListFilter.updateAndGetStatus("key1", 1);
        topListFilter.updateAndGetStatus("key2", 2);
        topListFilter.updateAndGetStatus("key3", 3);

        // Using a somewhat big delay in hopes of succeeding at the first try
        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .pollDelay(Duration.ofSeconds(1))
                .pollInterval(Duration.ofMillis(50))
                .until(() -> topListFilter.updateAndGetStatus("key4", 0));
    }

    @Test
    public void eventuallyForgetsSmallerEntriesAsLargerValuesAreAddedOverTimeWhenCapacityIsExhausted() {
        TopListFilter<String> topListFilter = new TopListFilter<>(3, Duration.ofSeconds(5));
        topListFilter.updateAndGetStatus("key1", 1);
        topListFilter.updateAndGetStatus("key2", 2);
        topListFilter.updateAndGetStatus("key3", 3);
        topListFilter.updateAndGetStatus("key4", 4);
        topListFilter.updateAndGetStatus("key5", 5);

        // Wait for the first reset taking into account all elements added above
        Awaitility.await()
                .atMost(Duration.ofMinutes(1))
                .pollDelay(Duration.ofMillis(5100))
                .pollInterval(Duration.ofMillis(50))
                .until(() -> !topListFilter.updateAndGetStatus("key1", 1));

        // Given we use a large reset interval, we assume the calls below happen quickly enough
        // and before the next reset
        assertThat(topListFilter.updateAndGetStatus("key2", 2)).isFalse();
        assertThat(topListFilter.updateAndGetStatus("key4", 3)).isTrue();
        assertThat(topListFilter.updateAndGetStatus("key4", 4)).isTrue();
        assertThat(topListFilter.updateAndGetStatus("key5", 5)).isTrue();
    }
}

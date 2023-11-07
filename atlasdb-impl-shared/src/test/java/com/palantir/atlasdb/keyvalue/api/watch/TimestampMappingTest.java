/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.api.watch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.Iterables;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import org.junit.jupiter.api.Test;

public class TimestampMappingTest {
    @Test
    public void singleLeaderMappingIsValid() {
        UUID id = UUID.randomUUID();
        ImmutableTimestampMapping timestampMapping = TimestampMapping.builder()
                .putTimestampMapping(1L, LockWatchVersion.of(id, 1))
                .build();
        assertThat(timestampMapping.leader()).isEqualTo(id);
    }

    @Test
    public void singleDistinctLeaderMappingIsValid() {
        UUID id = UUID.randomUUID();
        ImmutableTimestampMapping timestampMapping = TimestampMapping.builder()
                .putTimestampMapping(1L, LockWatchVersion.of(id, 1))
                .putTimestampMapping(2L, LockWatchVersion.of(id, 1))
                .build();
        assertThat(timestampMapping.leader()).isEqualTo(id);
    }

    @Test
    public void conflictingLeaderMappingIsInvalid() {
        ImmutableTimestampMapping.Builder builder = TimestampMapping.builder()
                .putTimestampMapping(1L, LockWatchVersion.of(UUID.randomUUID(), 1))
                .putTimestampMapping(2L, LockWatchVersion.of(UUID.randomUUID(), 1));

        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Input contains multiple distinct elements");
    }

    @Test
    public void emptyMappingIsInvalid() {
        assertThatThrownBy(() -> TimestampMapping.builder().build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid range");
    }

    @Test
    public void onlyIdenticalElements() {
        assertThat(TimestampMapping.identicalElement(List.of("test"))).isEqualTo("test");
        assertThat(TimestampMapping.identicalElement(Iterables.limit(Iterables.cycle("test"), 1000)))
                .isEqualTo("test");
        assertThat(TimestampMapping.identicalElement(Iterables.limit(Iterables.cycle(42L), 1000)))
                .isEqualTo(42L);
    }

    @Test
    public void emptyDistinctElements() {
        assertThatThrownBy(() -> TimestampMapping.identicalElement(List.of()))
                .isInstanceOf(NoSuchElementException.class);
    }

    @Test
    public void multipleDistinctElements() {
        assertThatThrownBy(() -> TimestampMapping.identicalElement(List.of("a", "b", "a")))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasMessageContaining("Input contains multiple distinct elements");
        assertThatThrownBy(() -> TimestampMapping.identicalElement(List.of("a", "a", "b")))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasMessageContaining("Input contains multiple distinct elements");
    }
}

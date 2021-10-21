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

package com.palantir.atlasdb.memory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.exception.NotInitializedException;
import com.palantir.refreshable.Refreshable;
import com.palantir.timestamp.TimestampService;
import java.time.Duration;
import java.util.Optional;
import org.awaitility.Awaitility;
import org.junit.Test;

public class InMemoryAsyncAtlasDbFactoryTest {
    private final AtlasDbFactory<?> factory = new InMemoryAsyncAtlasDbFactory();

    @Test
    public void syncInitKvsSynchronous() {
        KeyValueService kvs = createRawKeyValueService(false);
        assertThat(kvs.isInitialized()).isTrue();
        assertThat(kvs.getAllTableNames()).isEmpty();
    }

    @Test
    public void asyncInitKvsAsynchronous() {
        KeyValueService kvs = createRawKeyValueService(true);
        assertThat(kvs.isInitialized()).isFalse();
        assertThatThrownBy(kvs::getAllTableNames).isInstanceOf(NotInitializedException.class);

        Awaitility.await().atMost(Duration.ofSeconds(2)).until(kvs::isInitialized);
        assertThat(kvs.getAllTableNames()).isEmpty();
    }

    @Test
    public void syncInitTimestampServiceSynchronous() {
        TimestampService timestampService = factory.createManagedTimestampService(null, Optional.empty(), false);
        assertThat(timestampService.isInitialized()).isTrue();
        assertThat(timestampService.getFreshTimestamp()).isEqualTo(1L);
    }

    @Test
    public void asyncInitTimestampServiceWithReadyKvsSynchronous() {
        KeyValueService kvs = createRawKeyValueService(false);
        TimestampService timestampService = factory.createManagedTimestampService(kvs, Optional.empty(), true);
        assertThat(timestampService.isInitialized()).isTrue();
        assertThat(timestampService.getFreshTimestamp()).isEqualTo(1L);
    }

    @Test
    public void asyncInitTimestampServiceWithAsyncKvsAsynchronous() {
        KeyValueService kvs = createRawKeyValueService(true);
        TimestampService timestampService = factory.createManagedTimestampService(kvs, Optional.empty(), true);

        assertThat(timestampService.isInitialized()).isFalse();
        assertThatThrownBy(timestampService::getFreshTimestamp).isInstanceOf(NotInitializedException.class);

        Awaitility.await().atMost(Duration.ofSeconds(3)).until(timestampService::isInitialized);
        assertThat(timestampService.getFreshTimestamp()).isEqualTo(1L);
    }

    private KeyValueService createRawKeyValueService(boolean initializeAsync) {
        return factory.createRawKeyValueService(
                null,
                null,
                Refreshable.only(Optional.empty()),
                Optional.empty(),
                Optional.empty(),
                null,
                initializeAsync);
    }
}

/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.knowledge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.transaction.impl.TransactionTables;
import org.junit.Before;
import org.junit.Test;

public class DefaultFutileTimestampStoreTest {
    private static final long TIMESTAMP = 4L;

    private final KeyValueService keyValueService = new InMemoryKeyValueService(false);
    private final FutileTimestampStore futileTimestampStore = new DefaultFutileTimestampStore(keyValueService);

    @Before
    public void before() {
        TransactionTables.createTables(keyValueService);
    }

    @Test
    public void timestampsAreByDefaultNotFutile() {
        assertThat(futileTimestampStore.isTimestampKnownFutile(TIMESTAMP)).isFalse();
        assertThat(futileTimestampStore.isTimestampKnownFutile(33L)).isFalse();
        assertThat(futileTimestampStore.isTimestampKnownFutile(979L)).isFalse();
    }

    @Test
    public void timestampIsKnownFutileOnceMarkedAsSuch() {
        assertThat(futileTimestampStore.isTimestampKnownFutile(TIMESTAMP)).isFalse();
        futileTimestampStore.markFutile(TIMESTAMP);
        assertThat(futileTimestampStore.isTimestampKnownFutile(TIMESTAMP)).isTrue();
        assertThat(futileTimestampStore.isTimestampKnownFutile(TIMESTAMP + 1)).isFalse();
    }

    @Test
    public void markingTimestampsFutileIsIdempotent() {
        futileTimestampStore.markFutile(TIMESTAMP);
        assertThat(futileTimestampStore.isTimestampKnownFutile(TIMESTAMP)).isTrue();
        assertThatCode(() -> futileTimestampStore.markFutile(TIMESTAMP)).doesNotThrowAnyException();
    }
}
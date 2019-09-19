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

package com.palantir.atlasdb.keyvalue.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.exception.NotInitializedException;
import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;
import org.junit.Test;

public class AsyncInitializeableInMemoryKvsTest {

    @Test
    public void createSyncInitializationTest() {
        KeyValueService kvs = AsyncInitializeableInMemoryKvs.createAndStartInit(false);
        assertThat(kvs.isInitialized()).isTrue();
        assertThat(kvs.getAllTableNames()).isEmpty();
    }

    @Test
    public void createAsyncInitializationTest() {
        KeyValueService kvs = AsyncInitializeableInMemoryKvs.createAndStartInit(true);
        assertThat(kvs.isInitialized()).isFalse();
        assertThatThrownBy(kvs::getAllTableNames)
                .isInstanceOf(NotInitializedException.class);

        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(kvs::isInitialized);
        assertThat(kvs.getAllTableNames()).isEmpty();
    }
}

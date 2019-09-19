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
package com.palantir.atlasdb.keyvalue.cassandra;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.containers.UninitializedCassandraResource;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;
import org.junit.ClassRule;
import org.junit.Test;

public class CassandraKeyValueServiceAsyncInitializationTest {
    @ClassRule
    public static final UninitializedCassandraResource CASSANDRA = new UninitializedCassandraResource(
            CassandraKeyValueServiceAsyncInitializationTest.class);

    @Test
    public void cassandraKvsInitializesAsynchronously() throws IOException, InterruptedException {
        KeyValueService asyncInitializedKvs = CASSANDRA.getAsyncInitializeableKvs();
        assertThat(asyncInitializedKvs.isInitialized()).isFalse();

        CASSANDRA.initialize();

        Awaitility.await().atMost(35, TimeUnit.SECONDS).until(asyncInitializedKvs::isInitialized);
    }
}

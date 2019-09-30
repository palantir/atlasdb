/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra.async;

import java.util.concurrent.Executor;

import com.datastax.driver.core.Session;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.processors.AutoDelegate;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public interface CqlClientFactory {
    @AutoDelegate
    interface CqlResourceHandle extends AutoCloseable {
        Session session();
        Executor executor();
    }

    CqlClient constructClient(
            TaggedMetricRegistry taggedMetricRegistry,
            CassandraKeyValueServiceConfig config,
            boolean initializeAsync);
}

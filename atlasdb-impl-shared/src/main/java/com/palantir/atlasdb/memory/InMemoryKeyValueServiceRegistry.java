/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.transaction.impl.TransactionTables;
import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.ManagedTimestampService;

public final class InMemoryKeyValueServiceRegistry {
    private final LoadingCache<String, KeyValueService> keyValueServices = Caffeine.newBuilder()
            .build(_unused -> {
                InMemoryKeyValueService inMemoryKeyValueService = new InMemoryKeyValueService(false);
                TransactionTables.createTables(inMemoryKeyValueService);
                return inMemoryKeyValueService;
            });
    private final LoadingCache<String, ManagedTimestampService> timestampServices =
            Caffeine.newBuilder().build(_unused -> new InMemoryTimestampService());

    public KeyValueService getKeyValueService(String namespace) {
        return keyValueServices.get(namespace);
    }

    public ManagedTimestampService getManagedTimestampService(String namespace) {
        return timestampServices.get(namespace);
    }
}

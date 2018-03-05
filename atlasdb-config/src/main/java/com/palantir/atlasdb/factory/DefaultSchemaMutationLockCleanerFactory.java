/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.factory;

import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auto.service.AutoService;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.spi.SchemaMutationLockCleaner;
import com.palantir.atlasdb.spi.SchemaMutationLockCleanerFactory;

@AutoService(SchemaMutationLockCleanerFactory.class)
public class DefaultSchemaMutationLockCleanerFactory implements SchemaMutationLockCleanerFactory {
    private static final Logger log = LoggerFactory.getLogger(DefaultSchemaMutationLockCleanerFactory.class);
    private static final String CASSANDRA_KVS_TYPE = "cassandra";

    @Override
    public Supplier<SchemaMutationLockCleaner> getCleaner(KeyValueServiceConfig config) {
        return () -> NO_OP;
    }

    @Override
    public boolean supportsKVSType(String type) {
        return !type.equalsIgnoreCase(CASSANDRA_KVS_TYPE);
    }

    private static SchemaMutationLockCleaner NO_OP =
            () -> log.warn("Received request to clean schema mutation locks for non-Cassandra KVS, which is a no-op.");
}

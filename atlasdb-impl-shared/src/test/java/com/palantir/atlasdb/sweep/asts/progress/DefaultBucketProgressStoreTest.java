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

package com.palantir.atlasdb.sweep.asts.progress;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.KvsManager;
import com.palantir.atlasdb.transaction.api.TransactionManager;

public final class DefaultBucketProgressStoreTest extends AbstractBucketProgressStoreTest {
    public DefaultBucketProgressStoreTest() {
        super(new KvsManager() {
            @Override
            public KeyValueService getDefaultKvs() {
                return new InMemoryKeyValueService(false);
            }

            @Override
            public void registerKvs(KeyValueService kvs) {
                // Only used for this test
            }

            @Override
            public void registerTransactionManager(TransactionManager manager) {
                // Only used for this test
            }
        });
    }
}

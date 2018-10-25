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

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.api.TransactionManager;

public interface KvsManager {
    /**
     * Returns an instance of {@link KeyValueService} and registers it as a closeable resource. Subsequent invocations
     * of this method are guaranteed to return the same object
     */
    KeyValueService getDefaultKvs();
    /**
     * Register the given {@link KeyValueService} as a closeable resource.
     *
     * @param kvs the instance to be registered
     */
    void registerKvs(KeyValueService kvs);

    /**
     * Register the given {@link TransactionManager} as a closeable resource and for possible retrieval.
     *
     * @param manager the instance to be registered
     */
    void registerTransactionManager(TransactionManager manager);
}

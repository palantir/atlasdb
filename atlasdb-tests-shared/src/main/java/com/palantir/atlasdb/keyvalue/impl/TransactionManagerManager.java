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

import com.palantir.atlasdb.transaction.api.TransactionManager;
import java.util.Optional;

public interface TransactionManagerManager {
    /**
     * Register the given {@link TransactionManager} as a closeable resource and for possible retrieval.
     *
     * @param manager the instance to be registered
     */
    void registerTransactionManager(TransactionManager manager);

    /**
     * Returns the latest registered {@link TransactionManager} for this object. This method should generally only
     * be used in cases where no more than a single instance is ever registered.
     *
     * Correct usage in the case where multiple calls to {@link #registerTransactionManager(TransactionManager)} have
     * been made requires careful reasoning and is discouraged.
     *
     * @return an Optional containing the latest registered TransactionManager, or Optional.empty() if none were
     * registered.
     */
    Optional<TransactionManager> getLastRegisteredTransactionManager();
}

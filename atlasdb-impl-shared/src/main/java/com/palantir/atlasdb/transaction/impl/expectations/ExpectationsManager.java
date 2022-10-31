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

package com.palantir.atlasdb.transaction.impl.expectations;

import com.palantir.atlasdb.transaction.api.expectations.ExpectationsAwareTransaction;

public interface ExpectationsManager extends AutoCloseable {
    void register(ExpectationsAwareTransaction transaction);

    /*
     * Stop tracking a given transaction without running expectations callbacks.
     * Should not be used on transactions that are already in progress.
     * Use case is for when transactions are created and registered but thrown away unused.
     */
    void unregister(ExpectationsAwareTransaction transaction);

    /*
     * Mark transaction as concluded (aborted/succeeded), run expectations callbacks and stop tracking the transaction.
     * Unregistering transactions is done at best effort basis.
     * Running callbacks is guaranteed.
     */
    void markCompletion(ExpectationsAwareTransaction transaction);
}

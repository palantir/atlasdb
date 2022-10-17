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

package com.palantir.atlasdb.transaction.impl;

import com.palantir.atlasdb.transaction.api.ExpectationsAwareTransaction;

// todo aalouane TEX convert this to an implementation
public interface ExpectationsManager extends AutoCloseable {
    void scheduleMetricsUpdate(long delayMillis);

    void registerTransaction(ExpectationsAwareTransaction transaction);

    /*
     * Stop tracking a given transaction without running expectations callbacks.
     */
    void unregisterTransaction(ExpectationsAwareTransaction transaction);

    /*
     * Mark transaction as concluded (aborted/succeeded) and run expectations callbacks.
     */
    void markConcludedTransaction(ExpectationsAwareTransaction transaction);
}

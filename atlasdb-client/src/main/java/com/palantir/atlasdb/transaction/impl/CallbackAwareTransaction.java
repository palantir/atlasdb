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

import com.palantir.atlasdb.transaction.api.PreCommitCondition;
import com.palantir.atlasdb.transaction.api.Transaction;

/**
 * Allow consumers to register callbacks to be run on {@link Transaction#commit()} or {@link Transaction#abort()},
 * after a transaction has committed or aborted.
 * Callbacks are guaranteed to run even if a prior callback threw.
 * Callbacks are usually cleanup tasks, e.g. {@link PreCommitCondition#cleanup()} tasks.
 * Note this is different from {@link #onSuccess(Runnable)} as these tasks are run regardless of whether they succeeded
 * or failed.
 */
public interface CallbackAwareTransaction extends ExpectationsAwareTransaction {
    void onCommitOrAbort(Runnable runnable);
}

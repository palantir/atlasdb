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

public interface CallbackAwareTransaction extends ExpectationsAwareTransaction {
    /**
     * Allow consumers to register callbacks to be run on {@link Transaction#commit()} or {@link Transaction#abort()},
     * after a transaction has committed or aborted.
     * {@link #onCommitOrAbort(Runnable)} callbacks run before {@link #onSuccess(Runnable)} callbacks.
     * <p>
     * Callbacks are usually cleanup tasks, e.g. {@link PreCommitCondition#cleanup()}.
     * Since they are run after the transaction has committed or aborted, a callback exception does not affect
     * the status of the transaction.
     * In other words, it's possible for a transaction that committed successfully to have a callback that throws -
     * in this case {@link Transaction#commit()} would throw the callback's exception, but the transaction would still
     * commit successfully.
     * <p>
     * Callbacks are not atomic nor transactional - it's possible for a transaction to succeed and its callbacks to not
     * be triggered (e.g. the Java process was interrupted before).
     * Callbacks run even if a prior callback threw an exception.
     * Callbacks are run in the reverse order they were added - i.e. the last added callback will be run first.
     * Different from {@link #onSuccess(Runnable)}, {@link #onCommitOrAbort(Runnable)} callbacks are run if the
     * transaction succeeded or failed.
     */
    void onCommitOrAbort(Runnable runnable);
}

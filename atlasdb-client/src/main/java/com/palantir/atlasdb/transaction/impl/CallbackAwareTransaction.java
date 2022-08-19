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
import com.palantir.atlasdb.transaction.api.TransactionFailedException;
import com.palantir.atlasdb.transaction.service.TransactionService;

/**
 * Implementors of this interface provide more granular control over when the {@link #onSuccess(Runnable)} callbacks
 * are run. This can be used to e.g run {@link PreCommitCondition#cleanup()} <i>before</i> the onSuccess callbacks
 * are run.
 */
public interface CallbackAwareTransaction extends Transaction {
    void commitWithoutCallbacks() throws TransactionFailedException;

    void commitWithoutCallbacks(TransactionService transactionService) throws TransactionFailedException;

    void runSuccessCallbacksIfDefinitivelyCommitted();
}

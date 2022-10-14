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

import com.palantir.atlasdb.transaction.api.TransactionFailedException;
import com.palantir.atlasdb.transaction.service.TransactionService;

public abstract class ForwardingCallbackAwareTransaction extends ForwardingTransaction
        implements CallbackAwareTransaction {

    @Override
    public abstract CallbackAwareTransaction delegate();

    @Override
    public void commitWithoutCallbacks() throws TransactionFailedException {
        delegate().commitWithoutCallbacks();
    }

    @Override
    public void commitWithoutCallbacks(TransactionService transactionService) throws TransactionFailedException {
        delegate().commitWithoutCallbacks(transactionService);
    }

    @Override
    public void runSuccessCallbacksIfDefinitivelyCommitted() {
        delegate().runSuccessCallbacksIfDefinitivelyCommitted();
    }

    @Override
    public void runTransactionalExpectationsConsumerOperation() {
        delegate().runTransactionalExpectationsConsumerOperation();
    }
}

/**
 * // Copyright 2015 Palantir Technologies
 * //
 * // Licensed under the BSD-3 License (the "License");
 * // you may not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * // http://opensource.org/licenses/BSD-3-Clause
 * //
 * // Unless required by applicable law or agreed to in writing, software
 * // distributed under the License is distributed on an "AS IS" BASIS,
 * // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * // See the License for the specific language governing permissions and
 * // limitations under the License.
 */
package com.palantir.atlasdb.transaction.impl;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.atlasdb.transaction.service.TransactionService;

public class ShellAwareReadOnlyTransactionManager extends ReadOnlyTransactionManager {

    public ShellAwareReadOnlyTransactionManager(KeyValueService keyValueService,
                                                TransactionService transactionService,
                                                AtlasDbConstraintCheckingMode constraintCheckingMode) {
        super(keyValueService, transactionService, constraintCheckingMode);
    }

    @Override
    public <T, E extends Exception> T runTaskThrowOnConflict(TransactionTask<T, E> task) throws E,
            TransactionFailedRetriableException {
        throw new UnsupportedOperationException("AtlasDB Shell only supports write operations if connected" +
                " to lock and timestamp servers also.");
    }

}

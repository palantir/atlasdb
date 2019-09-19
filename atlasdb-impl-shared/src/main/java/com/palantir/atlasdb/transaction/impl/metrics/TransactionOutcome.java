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
package com.palantir.atlasdb.transaction.impl.metrics;

import com.palantir.atlasdb.transaction.api.LockAwareTransactionTask;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.service.TransactionService;
import java.util.function.Supplier;

/**
 * A TransactionOutcome describes an outcome resulting from a transaction. Note that a single transaction can
 * cause multiple outcomes (for example, a transaction that rolls back two other transactions and then successfully
 * commits will log one SUCCESSFUL_COMMIT outcome and two ROLLBACK_OTHER outcomes).
 *
 * Transactions that make it to commit time should log either SUCCESSFUL_COMMIT or FAILED_COMMIT. In addition to this,
 * various failure modes are tracked; if a transaction failed for multiple reasons, then we do not make guarantees on
 * which specific outcome(s) that transaction emits. We guarantee that the transaction emits at least one outcome
 * that is relevant.
 */
public enum TransactionOutcome {
    SUCCESSFUL_COMMIT,
    FAILED_COMMIT,
    ABORT,
    WRITE_WRITE_CONFLICT,
    READ_WRITE_CONFLICT,

    /**
     * Locks acquired as part of the AtlasDB transaction protocol (the immutable timestamp lock and row/cell locks)
     * have expired so we weren't able to commit. Note that locks acquired as part of TransactionManager APIs
     * (e.g. {@link TransactionManager#runTaskWithLocksWithRetry(Iterable, Supplier, LockAwareTransactionTask)})
     * are treated differently and are not covered by LOCKS_EXPIRED.
     */
    LOCKS_EXPIRED,

    /**
     * A pre-commit check has failed. This includes locks acquired as part of TransactionManager APIs expiring.
     */
    PRE_COMMIT_CHECK_FAILED,

    /**
     * putUnlessExists to the transactions table may have failed. The API of {@link TransactionService} doesn't allow
     * us to know whether the operation actually succeeded. Note that the case where we have lost our locks and then
     * someone else rolled us back is treated as LOCKS_EXPIRED, not PUT_UNLESS_EXISTS_FAILED.
     */
    PUT_UNLESS_EXISTS_FAILED,

    /**
     * Our transaction has rolled back another transaction (for example, if we read a value from a transaction
     * that started before our start timestamp and hasn't committed). This will be logged at most once for a given
     * transaction being rolled back, even if multiple transactions aim to rollback the same transaction.
     */
    ROLLBACK_OTHER
}

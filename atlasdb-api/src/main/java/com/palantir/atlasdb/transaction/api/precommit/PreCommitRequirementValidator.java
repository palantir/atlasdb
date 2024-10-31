/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.api.precommit;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.TransactionFailedException;
import java.util.Map;

public interface PreCommitRequirementValidator {
    /**
     * Throws a {@link TransactionFailedException} if a user-provided pre-commit condition is no longer valid.
     */
    void throwIfPreCommitConditionInvalid(long timestamp);

    /**
     * Throws a {@link TransactionFailedException} if a user-provided pre-commit condition is no longer valid,
     * considering the mutations that are about to be committed.
     */
    void throwIfPreCommitConditionInvalidAtCommitOnWriteTransaction(
            Map<TableReference, ? extends Map<Cell, byte[]>> mutations, long timestamp);

    /**
     * Throws a {@link TransactionFailedException} if the transaction can no longer commit; this can be because a
     * user pre-commit condition is no longer valid, or possibly because of other internal state such as commit
     * locks having expired.
     */
    void throwIfPreCommitRequirementsNotMet(long timestamp);

    /**
     * Throws a {@link TransactionFailedException} if the immutable timestamp lock or commit locks have expired.
     */
    void throwIfImmutableTsOrCommitLocksExpired();
}

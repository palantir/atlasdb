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

package com.palantir.atlasdb.transaction.impl.precommit;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.TransactionConfig;
import com.palantir.atlasdb.transaction.api.precommit.PreCommitRequirementValidator;
import com.palantir.atlasdb.transaction.api.precommit.ReadSnapshotValidator;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManager;
import java.util.function.Supplier;

public final class DefaultReadSnapshotValidator implements ReadSnapshotValidator {
    private final PreCommitRequirementValidator preCommitRequirementValidator;
    private final boolean validateLocksOnReads;
    private final SweepStrategyManager sweepStrategyManager;
    private final Supplier<TransactionConfig> transactionConfigSupplier;

    public DefaultReadSnapshotValidator(
            PreCommitRequirementValidator preCommitRequirementValidator,
            boolean validateLocksOnReads,
            SweepStrategyManager sweepStrategyManager,
            Supplier<TransactionConfig> transactionConfigSupplier) {
        this.preCommitRequirementValidator = preCommitRequirementValidator;
        this.validateLocksOnReads = validateLocksOnReads;
        this.sweepStrategyManager = sweepStrategyManager;
        this.transactionConfigSupplier = transactionConfigSupplier;
    }

    @Override
    public ValidationState throwIfPreCommitRequirementsNotMetOnRead(
            TableReference tableRef, long timestamp, boolean allPossibleCellsReadAndPresent) {
        if (isValidationNecessaryOnReads(tableRef, allPossibleCellsReadAndPresent)) {
            preCommitRequirementValidator.throwIfPreCommitRequirementsNotMet(null, timestamp);
            return ValidationState.COMPLETELY_VALIDATED;
        }

        if (allPossibleCellsReadAndPresent) {
            return ValidationState.COMPLETELY_VALIDATED;
        } else {
            return ValidationState.NOT_COMPLETELY_VALIDATED;
        }
    }

    @Override
    public boolean doesTableRequirePreCommitValidation(TableReference tableRef, boolean allReadsCompleteOrValidated) {
        return requiresImmutableTimestampLocking(tableRef, allReadsCompleteOrValidated);
    }

    private boolean isValidationNecessaryOnReads(TableReference tableRef, boolean allPossibleCellsReadAndPresent) {
        return validateLocksOnReads && requiresImmutableTimestampLocking(tableRef, allPossibleCellsReadAndPresent);
    }

    private boolean requiresImmutableTimestampLocking(TableReference tableRef, boolean allPossibleCellsReadAndPresent) {
        return sweepStrategyManager.get(tableRef).mustCheckImmutableLock(allPossibleCellsReadAndPresent)
                || transactionConfigSupplier.get().lockImmutableTsOnReadOnlyTransactions();
    }
}

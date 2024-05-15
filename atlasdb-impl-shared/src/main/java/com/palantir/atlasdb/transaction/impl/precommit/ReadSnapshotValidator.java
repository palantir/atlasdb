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

/**
 * The AtlasDB read protocol in certain circumstances requires that we check that pre-commit requirements are
 * still being met before a transaction completes. Depending on the schema of the relevant tables, this may need
 * to happen at different times (e.g., in an effort to prevent dirty reads). For example, for tables that are
 * thoroughly swept, we need to make sure that sweep has not deleted any cells that we read versions of.
 */
public interface ReadSnapshotValidator {
    /**
     * Checks if pre-commit requirements are met if necessary, throwing an exception if they are not. Also returns
     * whether a further validation check may be necessary (i.e., if the read was not completely validated).
     * Typically, further validation is necessary if validation was not performed and if the read was to a table
     * that requires pre-commit validation, but not validations on reads themselves.
     *
     * By default, we validate locks on reads to tables that require pre-commit validation, so under default
     * settings all reads should be completely validated, whether because the validation was performed, all possible
     * cells were read and present, or it was skipped because the table as a whole does not require pre-commit
     * validation.
     */
    ValidationState throwIfPreCommitRequirementsNotMetOnRead(
            TableReference tableRef, long timestamp, boolean allPossibleCellsReadAndPresent);

    boolean doesTableRequirePreCommitValidation(TableReference tableRef, boolean allPossibleCellsReadAndPresent);

    void disableValidatingLocksOnReads();

    enum ValidationState {
        COMPLETELY_VALIDATED,
        NOT_COMPLETELY_VALIDATED
    }
}

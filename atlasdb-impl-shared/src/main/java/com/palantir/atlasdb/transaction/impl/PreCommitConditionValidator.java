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

package com.palantir.atlasdb.transaction.impl;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.lock.v2.LockToken;
import java.util.Map;
import java.util.Optional;

public interface PreCommitConditionValidator {
    // TODO (jkong): The boolean means "are there unvalidated reads"?
    boolean throwIfPreCommitRequirementsNotMetOnRead(
            TableReference tableRef, long timestamp, boolean allPossibleCellsReadAndPresent);

    void throwIfPreCommitConditionInvalidAtCommitOnWriteTransaction(
            Map<TableReference, ? extends Map<Cell, byte[]>> mutations, long timestamp);

    void throwIfPreCommitRequirementsNotMet(Optional<LockToken> commitLocksToken, long timestamp);

    void throwIfImmutableTsOrCommitLocksExpired(Optional<LockToken> commitLocksToken);
}

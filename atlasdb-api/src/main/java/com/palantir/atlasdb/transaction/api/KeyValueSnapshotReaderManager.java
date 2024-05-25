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

package com.palantir.atlasdb.transaction.api;

import com.palantir.atlasdb.transaction.api.precommit.PreCommitRequirementValidator;
import com.palantir.atlasdb.transaction.api.precommit.ReadSnapshotValidator;
import java.util.function.LongSupplier;
import org.immutables.value.Value;

public interface KeyValueSnapshotReaderManager {
    KeyValueSnapshotReader createKeyValueSnapshotReader(TransactionContext transactionContext);

    @Value.Immutable
    interface TransactionContext {
        LongSupplier startTimestampSupplier();

        TransactionReadSentinelBehavior transactionReadSentinelBehavior();

        CommitTimestampLoader commitTimestampLoader();

        PreCommitRequirementValidator preCommitRequirementValidator();

        ReadSnapshotValidator readSnapshotValidator();

        KeyValueSnapshotEventRecorder keyValueSnapshotEventRecorder();
    }
}

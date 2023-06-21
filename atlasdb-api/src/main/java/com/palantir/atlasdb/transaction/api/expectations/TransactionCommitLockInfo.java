/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.api.expectations;

import org.immutables.value.Value;

/**
 * Data about the amount of commit locks requested by a transaction.
 */
@Value.Immutable
public interface TransactionCommitLockInfo {

    /**
     * The number of cell-level locks requested as part of the commit protocol (excluding any user-defined locking).
     */
    long cellCommitLocksRequested();

    /**
     * The number of row-level locks requested as part of the commit protocol (excluding any user-defined locking).
     * For write transactions, this includes the single row lock that is requested for the transaction table itself.
     */
    long rowCommitLocksRequested();
}

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

/**
 * Ways in which an individual value can be uncommitted for purposes of the AtlasDB protocol.
 */
enum UncommittedValueType {
    /**
     * The value is not committed because it was explicitly aborted.
     */
    EXPLICIT_ABORT,
    /**
     * The value is not committed because there is no record of it being known to be committed in the transactions
     * table. It is possible for the value to subsequently become committed or aborted; readers for purposes of the
     * transaction protocol must attempt to roll back.
     */
    NO_ENTRY_IN_TRANSACTIONS_TABLE;
}

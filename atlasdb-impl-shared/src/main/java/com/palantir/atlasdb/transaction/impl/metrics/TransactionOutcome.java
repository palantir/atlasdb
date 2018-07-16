/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.transaction.impl.metrics;

/**
 * A TransactionOutcome describes the effect that a trans
 */
public enum TransactionOutcome {
    SUCCESSFUL_COMMIT,
    FAILED_COMMIT,
    ABORT,
    WRITE_WRITE_CONFLICT,
    READ_WRITE_CONFLICT,
    LOCKS_EXPIRED,
    PRE_COMMIT_CHECK_FAILED,
    PUT_UNLESS_EXISTS_FAILED,
    ROLLBACK_OTHER
}

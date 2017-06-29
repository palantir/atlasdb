/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl;

/**
 * A validation to be run immediately before committing a transaction.
 */
public interface PreCommitValidation {

    /**
     * Checks that any required conditions for committing still hold. The intended use case at
     * time of writing is to validate that advisory lock service locks are still valid. In this case,
     * implementations should throw a {@link com.palantir.atlasdb.transaction.api.TransactionLockTimeoutException}
     * if locks are expired, so that the transaction can be retried.
     */
    void check();

    PreCommitValidation NO_OP = () -> { };

}

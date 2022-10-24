/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl.expectations;

import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.expectations.ExpectationsConfig;
import com.palantir.atlasdb.transaction.api.expectations.ExpectationsStatistics;
import com.palantir.atlasdb.transaction.api.expectations.TransactionReadInfo;
import java.util.Set;

public interface ExpectationsAwareTransaction extends Transaction {
    ExpectationsConfig expectationsConfig();

    long getAgeMillis();

    TransactionReadInfo getReadInfo();

    /*
     * A consistent view of ExpectationsStatistics is not guaranteed if the user interacts with the transaction
     * post-commit/post-abort or outside the user task.
     */
    ExpectationsStatistics getCallbackStatistics();

    /*
     * A consistent view of ExpectationsStatistics is not guaranteed if the user interacts with the transaction
     * post-commit/post-abort or outside the user task.
     */
    void runExpectationsCallbacks();

    Set<ExpectationsViolation> checkAndGetViolations();
}

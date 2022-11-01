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

package com.palantir.atlasdb.transaction.impl;

import com.palantir.atlasdb.transaction.api.expectations.ExpectationsAwareTransaction;
import com.palantir.atlasdb.transaction.api.expectations.ExpectationsConfig;
import com.palantir.atlasdb.transaction.api.expectations.ExpectationsStatistics;
import com.palantir.atlasdb.transaction.api.expectations.ExpectationsViolation;
import com.palantir.atlasdb.transaction.api.expectations.TransactionReadInfo;
import java.util.Set;

public abstract class ForwardingExpectationsAwareTransaction extends ForwardingTransaction
        implements ExpectationsAwareTransaction {
    @Override
    public abstract ExpectationsAwareTransaction delegate();

    @Override
    public ExpectationsConfig expectationsConfig() {
        return delegate().expectationsConfig();
    }

    @Override
    public long getAgeMillis() {
        return delegate().getAgeMillis();
    }

    @Override
    public long getAgeMillisAndFreezeTimer() {
        return delegate().getAgeMillisAndFreezeTimer();
    }

    @Override
    public TransactionReadInfo getReadInfo() {
        return delegate().getReadInfo();
    }

    @Override
    public ExpectationsStatistics getCallbackStatistics() {
        return delegate().getCallbackStatistics();
    }

    @Override
    public void runExpectationsCallbacks() {
        delegate().runExpectationsCallbacks();
    }

    @Override
    public Set<ExpectationsViolation> checkAndGetViolations() {
        return delegate().checkAndGetViolations();
    }

    @Override
    public void reportExpectationsCollectedData() {
        delegate().reportExpectationsCollectedData();
    }
}

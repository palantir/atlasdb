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

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.ExpectationsAwareTransaction;
import com.palantir.atlasdb.transaction.api.ExpectationsConfig;
import com.palantir.atlasdb.transaction.api.ExpectationsStatistics;

public abstract class ExpectationsAwareForwardingTransaction extends ForwardingTransaction
        implements ExpectationsAwareTransaction {
    @Override
    public abstract ExpectationsAwareTransaction delegate();

    @Override
    public void runExpectationsCallbacks(ExpectationsStatistics stats) {
        delegate().runExpectationsCallbacks(stats);
    }

    @Override
    public long getBytesRead() {
        return delegate().getBytesRead();
    }

    @Override
    public ImmutableMap<TableReference, Long> getBytesReadByTable() {
        return delegate().getBytesReadByTable();
    }

    @Override
    public ExpectationsConfig expectationsConfig() {
        return delegate().expectationsConfig();
    }
}

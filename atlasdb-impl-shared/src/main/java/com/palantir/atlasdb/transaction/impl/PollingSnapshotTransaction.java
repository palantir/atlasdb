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

import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;

public class PollingSnapshotTransaction extends ForwardingTransaction {

    private SnapshotTransaction delegate;

    public PollingSnapshotTransaction(SnapshotTransaction delegate, boolean pollForKvs) {
        this.delegate = delegate;
    }

    @Override
    public SnapshotTransaction delegate() {
        return delegate;
    }

    Multimap<TableReference, Cell> getCellsToScrubImmediately() {
        return delegate().getCellsToScrubImmediately();
    }

    long getCommitTimestamp() {
        return delegate().getCommitTimestamp();
    }
}

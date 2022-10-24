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

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.expectations.TransactionReadInfo;

// todo(aalouane)
public abstract class KeyValueServiceDataTracker {
    public abstract TransactionReadInfo getReadInfo();

    public abstract ImmutableMap<TableReference, TransactionReadInfo> getReadInfoByTable();

    /*
     * Tracks all data read in one kvs read method call.
     */
    public abstract void registerKvsGetMethodRead(TableReference tableRef, String methodName, long bytesRead);

    /*
     * Track some, but not necessarily all, data read in some kvs read method call.
     */
    public abstract void registerKvsGetPartialRead(TableReference tableRef, long bytesRead);

    public abstract void incrementKvsReadCallCount(TableReference tableRef);
    ;
    ;
    ;
}

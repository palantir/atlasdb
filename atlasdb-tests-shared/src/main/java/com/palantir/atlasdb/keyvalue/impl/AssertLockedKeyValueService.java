/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.keyvalue.impl;

import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.common.base.Throwables;
import com.palantir.lock.AtlasRowLockDescriptor;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockService;
import com.palantir.logsafe.Preconditions;
import java.util.Arrays;
import java.util.Map;
import java.util.SortedMap;

public class AssertLockedKeyValueService extends ForwardingKeyValueService {
    final KeyValueService delegate;
    final LockService lockService;

    public AssertLockedKeyValueService(KeyValueService delegate, LockService lockService) {
        this.delegate = delegate;
        this.lockService = lockService;
    }

    @Override
    public KeyValueService delegate() {
        return delegate;
    }

    @Override
    public void put(TableReference tableRef, Map<Cell, byte[]> values, long timestamp) {

        if (tableRef.equals(TransactionConstants.TRANSACTION_TABLE)) {
            SortedMap<LockDescriptor, LockMode> mapToAssertLockHeld = Maps.newTreeMap();
            SortedMap<LockDescriptor, LockMode> mapToAssertLockNotHeld = Maps.newTreeMap();
            for (Map.Entry<Cell, byte[]> e : values.entrySet()) {
                LockDescriptor descriptor = AtlasRowLockDescriptor.of(tableRef.getQualifiedName(),
                        e.getKey().getRowName());
                if (Arrays.equals(e.getValue(),
                        TransactionConstants.getValueForTimestamp(TransactionConstants.FAILED_COMMIT_TS))) {
                    mapToAssertLockNotHeld.put(descriptor, LockMode.READ);
                } else {
                    mapToAssertLockHeld.put(descriptor, LockMode.READ);
                }
            }

            try {
                if (!mapToAssertLockHeld.isEmpty()) {
                    LockRequest request = LockRequest.builder(mapToAssertLockHeld)
                            .doNotBlock()
                            .lockAsManyAsPossible()
                            .build();
                    LockRefreshToken lock = lockService.lock(LockClient.ANONYMOUS.getClientId(), request);
                    Preconditions.checkArgument(lock == null, "these should already be held");
                }

                if (!mapToAssertLockNotHeld.isEmpty()) {
                    LockRequest request = LockRequest.builder(mapToAssertLockNotHeld).doNotBlock().build();
                    LockRefreshToken lock = lockService.lock(LockClient.ANONYMOUS.getClientId(), request);
                    Preconditions.checkArgument(lock != null, "these should already be waited for");
                }
            } catch (InterruptedException e) {
                throw Throwables.throwUncheckedException(e);
            }
        }

        super.put(tableRef, values, timestamp);
    }
}

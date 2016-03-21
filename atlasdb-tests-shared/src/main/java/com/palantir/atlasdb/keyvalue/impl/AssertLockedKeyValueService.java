/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.impl;

import java.util.Arrays;
import java.util.Map;
import java.util.SortedMap;

import org.apache.commons.lang.Validate;

import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.common.base.Throwables;
import com.palantir.lock.AtlasRowLockDescriptor;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.RemoteLockService;

public class AssertLockedKeyValueService extends ForwardingKeyValueService {
    final KeyValueService delegate;
    final RemoteLockService lockService;

    public AssertLockedKeyValueService(KeyValueService delegate, RemoteLockService lockService) {
        this.delegate = delegate;
        this.lockService = lockService;
    }

    @Override
    protected KeyValueService delegate() {
        return delegate;
    }

    @Override
    public void put(String tableName, Map<Cell, byte[]> values, long timestamp) {

        if (tableName.equals(TransactionConstants.TRANSACTION_TABLE)) {
            SortedMap<LockDescriptor, LockMode> mapToAssertLockHeld = Maps.newTreeMap();
            SortedMap<LockDescriptor, LockMode> mapToAssertLockNotHeld = Maps.newTreeMap();
            for (Map.Entry<Cell, byte[]> e : values.entrySet()) {
                if (Arrays.equals(e.getValue(), TransactionConstants.getValueForTimestamp(TransactionConstants.FAILED_COMMIT_TS))) {
                    mapToAssertLockNotHeld.put(AtlasRowLockDescriptor.of(tableName, e.getKey().getRowName()), LockMode.READ);
                } else {
                    mapToAssertLockHeld.put(AtlasRowLockDescriptor.of(tableName, e.getKey().getRowName()), LockMode.READ);
                }
            }

            try {
                if (!mapToAssertLockHeld.isEmpty()) {
                    LockRequest request = LockRequest.builder(mapToAssertLockHeld).doNotBlock().lockAsManyAsPossible().build();
                    LockRefreshToken lock = lockService.lock(LockClient.ANONYMOUS.getClientId(), request);
                    Validate.isTrue(lock == null, "these should already be held");
                }

                if (!mapToAssertLockNotHeld.isEmpty()) {
                    LockRequest request = LockRequest.builder(mapToAssertLockNotHeld).doNotBlock().build();
                    LockRefreshToken lock = lockService.lock(LockClient.ANONYMOUS.getClientId(), request);
                    Validate.isTrue(lock != null, "these should already be waited for");
                }
            } catch (InterruptedException e) {
                throw Throwables.throwUncheckedException(e);
            }
        }

        super.put(tableName, values, timestamp);
    }
}

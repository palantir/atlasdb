/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.lock;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.lock.ByteArrayLockDescriptor;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;

public class SimpleLockResource implements LockResource {
    private final TransactionManager transactionManager;

    public SimpleLockResource(TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    @Override
    public LockResponse lockWithTimelock(int lockDescriptorSize) {
        LockRequest request = LockRequest.of(ImmutableSet.of(generateDescriptorOfSize(lockDescriptorSize)), 1_000);
        LockResponse response = transactionManager.getTimelockService().lock(request);
        if (response.wasSuccessful()) {
            transactionManager.getTimelockService().unlock(ImmutableSet.of(response.getToken()));
        }
        return response;
    }

    @Override
    public LockRefreshToken lockWithLockService(int lockDescriptorSize) throws InterruptedException {
        com.palantir.lock.LockRequest request = com.palantir.lock.LockRequest.builder(
                ImmutableSortedMap.of(generateDescriptorOfSize(lockDescriptorSize), LockMode.WRITE)).build();
        LockRefreshToken response = transactionManager.getLockService().lock("test", request);
        if (response != null) {
            transactionManager.getLockService().unlock(response);
        }
        return response;
    }

    private LockDescriptor generateDescriptorOfSize(int size) {
        return ByteArrayLockDescriptor.of(new byte[size]);
    }
}

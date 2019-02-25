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

import java.security.SecureRandom;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

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
    private static final SecureRandom GENERATOR = new SecureRandom();
    private final TransactionManager transactionManager;

    public SimpleLockResource(TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    @Override
    public LockResponse lockWithTimelock(int numLocks, int descriptorSize) {
        Set<LockDescriptor> descriptors = generateDescriptors(numLocks, descriptorSize).collect(Collectors.toSet());
        LockRequest request = LockRequest.of(descriptors, 1_000);
        LockResponse response = transactionManager.getTimelockService().lock(request);
        if (response.wasSuccessful()) {
            transactionManager.getTimelockService().unlock(ImmutableSet.of(response.getToken()));
        }
        return response;
    }

    @Override
    public LockRefreshToken lockWithLockService(int numLocks, int descriptorSize) throws InterruptedException {
        ImmutableSortedMap.Builder<LockDescriptor, LockMode> builder = ImmutableSortedMap.naturalOrder();
        generateDescriptors(numLocks, descriptorSize).forEach(descriptor -> builder.put(descriptor, LockMode.WRITE));
        com.palantir.lock.LockRequest request = com.palantir.lock.LockRequest.builder(builder.build()).build();
        LockRefreshToken response = transactionManager.getLockService().lock("test", request);
        if (response != null) {
            transactionManager.getLockService().unlock(response);
        }
        return response;
    }

    private Stream<LockDescriptor> generateDescriptors(int numDescriptors, int descriptorSize) {
        return IntStream.range(0, numDescriptors).mapToObj(ignore -> generateDescriptorOfSize(descriptorSize));
    }

    private LockDescriptor generateDescriptorOfSize(int size) {
        byte[] bytes = new byte[size];
        GENERATOR.nextBytes(bytes);
        return ByteArrayLockDescriptor.of(bytes);
    }
}

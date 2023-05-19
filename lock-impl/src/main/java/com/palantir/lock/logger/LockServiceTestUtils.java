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
package com.palantir.lock.logger;

import com.google.common.collect.ImmutableSortedMap;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockCollections;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRequest;
import com.palantir.lock.StringLockDescriptor;
import java.math.BigInteger;
import java.util.Map;

public class LockServiceTestUtils {

    public static HeldLocksToken getFakeHeldLocksToken(
            String clientName, String requestingThread, BigInteger tokenId, Map<String, LockMode> descriptors) {
        ImmutableSortedMap<LockDescriptor, LockMode> lockDescriptorLockMode = getLockDescriptorLockMode(descriptors);

        return new HeldLocksToken(
                tokenId,
                LockClient.of(clientName),
                System.currentTimeMillis(),
                System.currentTimeMillis(),
                LockCollections.of(lockDescriptorLockMode),
                LockRequest.getDefaultLockTimeout(),
                0L,
                requestingThread);
    }

    public static ImmutableSortedMap<LockDescriptor, LockMode> getLockDescriptorLockMode(
            Map<String, LockMode> descriptors) {
        ImmutableSortedMap.Builder<LockDescriptor, LockMode> builder = ImmutableSortedMap.naturalOrder();
        for (Map.Entry<String, LockMode> descriptorAndMode : descriptors.entrySet()) {
            LockDescriptor descriptor1 = StringLockDescriptor.of(descriptorAndMode.getKey());
            builder.put(descriptor1, descriptorAndMode.getValue());
        }
        return builder.buildOrThrow();
    }
}

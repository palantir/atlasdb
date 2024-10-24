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
package com.palantir.atlasdb.timelock.lock;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.timelock.timestampleases.TimestampLeaseMetrics;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

public class LockCollectionTest {

    private final LockManager lockManager =
            LockManager.create(TimestampLeaseMetrics.of(new DefaultTaggedMetricRegistry()));

    @Test
    public void createsLocksOnDemand() {
        Set<LockDescriptor> descriptors = descriptors("foo", "bar");

        List<AsyncLock> locks = lockManager.getAllExclusiveLocks(descriptors).get();

        assertThat(locks).hasSize(2);
        assertThat(ImmutableSet.copyOf(locks)).hasSize(2);
    }

    @Test
    public void returnsSameLockForMultipleRequests() {
        Set<LockDescriptor> descriptors = descriptors("foo", "bar");

        List<AsyncLock> locks1 = lockManager.getAllExclusiveLocks(descriptors).get();
        List<AsyncLock> locks2 = lockManager.getAllExclusiveLocks(descriptors).get();

        assertThat(locks1).containsExactlyElementsOf(locks2);
    }

    @Test
    public void returnsLocksInOrder() {
        List<LockDescriptor> orderedDescriptors = IntStream.range(0, 10)
                .mapToObj(i -> UUID.randomUUID().toString())
                .map(StringLockDescriptor::of)
                .sorted()
                .collect(Collectors.toList());
        List<AsyncLock> expectedOrder = orderedDescriptors.stream()
                .map(descriptor -> lockManager.getAllExclusiveLocks(ImmutableSet.of(descriptor)))
                .map(orderedLocks -> orderedLocks.get().get(0))
                .collect(Collectors.toList());

        List<AsyncLock> actualOrder = lockManager
                .getAllExclusiveLocks(ImmutableSet.copyOf(orderedDescriptors))
                .get();

        assertThat(actualOrder).containsExactlyElementsOf(expectedOrder);
    }

    private static Set<LockDescriptor> descriptors(String... names) {
        return Arrays.stream(names).map(StringLockDescriptor::of).collect(Collectors.toSet());
    }
}

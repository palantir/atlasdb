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

package com.palantir.atlasdb.timelock.lock;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.StringLockDescriptor;

public class LockCollectionTest {

    private final LockCollection lockCollection = new LockCollection();

    @Test
    public void createsLocksOnDemand() {
        Set<LockDescriptor> descriptors = descriptors("foo", "bar");

        List<AsyncLock> locks = lockCollection.getSorted(descriptors);

        assertThat(locks.size()).isEqualTo(2);
        assertThat(ImmutableSet.copyOf(locks).size()).isEqualTo(2);
    }

    @Test
    public void returnsSameLockForMultipleRequests() {
        Set<LockDescriptor> descriptors = descriptors("foo", "bar");

        List<AsyncLock> locks1 = lockCollection.getSorted(descriptors);
        List<AsyncLock> locks2 = lockCollection.getSorted(descriptors);

        assertThat(locks1).isEqualTo(locks2);
    }

    @Test
    public void returnsLocksInOrder() {
        List<LockDescriptor> orderedDescriptors = IntStream.range(0, 10)
                .mapToObj(i -> UUID.randomUUID().toString())
                .map(StringLockDescriptor::of)
                .sorted().collect(Collectors.toList());
        List<AsyncLock> expectedOrder = orderedDescriptors.stream()
                .map(descriptor -> lockCollection.getSorted(ImmutableSet.of(descriptor)))
                .map(set -> set.iterator().next())
                .collect(Collectors.toList());

        List<AsyncLock> actualOrder = lockCollection.getSorted(ImmutableSet.copyOf(orderedDescriptors));

        assertThat(actualOrder).isEqualTo(expectedOrder);
    }

    private Set<LockDescriptor> descriptors(String... names) {
        return Arrays.stream(names)
                .map(StringLockDescriptor::of)
                .collect(Collectors.toSet());
    }

}

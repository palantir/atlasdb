/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.timelock.atomix;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.junit.Test;

import io.atomix.Atomix;
import io.atomix.group.DistributedGroup;
import io.atomix.variables.DistributedLong;
import io.atomix.variables.DistributedValue;

public class DistributedValuesTest {
    private static final Atomix ATOMIX = mock(Atomix.class);

    @Test
    public void sameKeyShouldBeUsedEveryTimeForGettingTheLeaderId() {
        String leaderKey1 = getAtomixKey(DistributedValues::getLeaderInfo);
        String leaderKey2 = getAtomixKey(DistributedValues::getLeaderInfo);

        assertThat(leaderKey1).isEqualTo(leaderKey2);
    }

    @Test
    public void sameKeyShouldBeUsedEveryTimeForGettingTheTimeLockGroup() {
        String groupKey1 = getAtomixKey(DistributedValues::getTimeLockGroup);
        String groupKey2 = getAtomixKey(DistributedValues::getTimeLockGroup);

        assertThat(groupKey1).isEqualTo(groupKey2);
    }

    @Test
    public void sameKeyShouldBeUsedEveryTimeForGettingTheTimestampValueForTheSameClient() {
        String timestampClientKey1 = getAtomixKey(atomix -> DistributedValues.getTimestampForClient(atomix, "client1"));
        String timestampClientKey2 = getAtomixKey(atomix -> DistributedValues.getTimestampForClient(atomix, "client1"));

        assertThat(timestampClientKey1).isEqualTo(timestampClientKey2);
    }

    @Test
    public void differentKeysShouldBeUsedForGettingTheTimestampValueForDifferentClient() {
        String timestampClientKey1 = getAtomixKey(atomix -> DistributedValues.getTimestampForClient(atomix, "client1"));
        String timestampClientKey2 = getAtomixKey(atomix -> DistributedValues.getTimestampForClient(atomix, "client2"));

        assertThat(timestampClientKey1).isNotEqualTo(timestampClientKey2);
    }

    private String getAtomixKey(Consumer<Atomix> consumer) {
        AtomicReference<String> key = new AtomicReference<>();

        when(ATOMIX.getValue(anyString())).thenAnswer(invocation -> {
            key.set((String) invocation.getArguments()[0]);
            return CompletableFuture.completedFuture(mock(DistributedValue.class));
        });
        when(ATOMIX.getGroup(anyString())).thenAnswer(invocation -> {
            key.set((String) invocation.getArguments()[0]);
            return CompletableFuture.completedFuture(mock(DistributedGroup.class));
        });
        when(ATOMIX.getLong(anyString())).thenAnswer(invocation -> {
            key.set((String) invocation.getArguments()[0]);
            return CompletableFuture.completedFuture(mock(DistributedLong.class));
        });

        consumer.accept(ATOMIX);

        return key.get();
    }
}

/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.api.watch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.UnlockEvent;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;

public final class VersionedEventStoreTest {

    private static final LockWatchEvent EVENT_1 = UnlockEvent.builder(ImmutableSet.of()).build(1L);
    private static final LockWatchEvent EVENT_2 = UnlockEvent.builder(ImmutableSet.of()).build(2L);
    private static final LockWatchEvent EVENT_3 = UnlockEvent.builder(ImmutableSet.of()).build(3L);
    private static final LockWatchEvent EVENT_4 = UnlockEvent.builder(ImmutableSet.of()).build(4L);

    private VersionedEventStore eventStore;

//    @Before
//    public void before() {
//        eventStore = new VersionedEventStore();
//    }
//
//    @Test
//    public void cannotGetFirstKeyFromEmptyStore() {
//        assertThatThrownBy(() -> eventStore.getFirstKey())
//                .isExactlyInstanceOf(SafeIllegalStateException.class)
//                .hasMessage("Cannot get first key from empty map");
//    }
//
//    @Test
//    public void cannotGetLastKeyFromEmptyStore() {
//        assertThatThrownBy(() -> eventStore.getLastKey())
//                .isExactlyInstanceOf(SafeIllegalStateException.class)
//                .hasMessage("Cannot get last key from empty map");
//    }
//
//    @Test
//    public void getElementsUpToExclusiveDoesNotIncludeEndVersion() {
//        eventStore.putAll(events);
//        eventStore.putAll(events);
//        eventStore.putAll(events);
//        Set<Map.Entry<Long, LockWatchEvent>> elements = eventStore.getAndRemoveElementsUpToExclusive(3L);
//        assertThat(elements.stream().map(Map.Entry::getKey)).containsExactly(1L, 2L);
//    }
//
//    @Test
//    public void clearElementsUpToExclusiveDoesNotIncludeEndVersion() {
//        eventStore.putAll(events);
//        eventStore.putAll(events);
//        eventStore.putAll(events);
//        eventStore.clearElementsUpToExclusive(3L);
//        assertThat(eventStore.getFirstKey()).isEqualTo(3L);
//    }
//
//    @Test
//    public void hasFloorKeyReturnsFalseWhenKeyBelowFirstKey() {
//        eventStore.putAll(events);
//        assertThat(eventStore.contains(9L)).isFalse();
//    }
//
//    @Test
//    public void hasFloorKeyReturnsTrueForAnyLargerOrEqualKey() {
//        eventStore.putAll(events);
//        assertThat(eventStore.contains(10L)).isTrue();
//        assertThat(eventStore.contains(9999L)).isTrue();
//    }
//
//    @Test
//    public void getEventsBetweenVersionsReturnsInclusiveOnBounds() {
//        eventStore.putAll(events);
//        eventStore.putAll(events);
//        eventStore.putAll(events);
//        eventStore.putAll(events);
//
//        assertThat(eventStore.getEventsBetweenVersionsInclusive(2L, 3L)).containsExactly(EVENT_2, EVENT_3);
//    }
}

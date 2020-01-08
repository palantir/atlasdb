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

package com.palantir.atlasdb.timelock.lock.watch;

import static org.assertj.core.api.Assertions.assertThat;

import static com.palantir.atlasdb.timelock.lock.watch.LockEventLogImpl.WINDOW_SIZE;

import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Test;

import com.palantir.lock.LockDescriptor;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.LockEvent;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.LockWatchStateUpdate;

public class LockEventLogImplTest {
    private long nextSequence = 0;
    private final LockEventLog log = new LockEventLogImpl();

    @Test
    public void test() {
        LockWatchEvent loggedEvent = logLocks(100);

        LockWatchStateUpdate updateWithNoFromVersion = log.getLogDiff(OptionalLong.empty());
        assertThat(updateWithNoFromVersion.success()).isFalse();
        assertThat(updateWithNoFromVersion.events()).isEmpty();
        assertThat(updateWithNoFromVersion.lastKnownVersion()).isEqualTo(OptionalLong.of(loggedEvent.size() - 1));
    }

    @Test
    public void getFromBeginningSingleEvent() {
        LockWatchEvent loggedEvent = logLocks(100);

        LockWatchStateUpdate updateWithNoFromVersion = log.getLogDiff(OptionalLong.of(-1));
        assertThat(updateWithNoFromVersion.success()).isTrue();
        assertThat(updateWithNoFromVersion.events()).containsExactly(loggedEvent);
        assertThat(updateWithNoFromVersion.lastKnownVersion()).isEqualTo(OptionalLong.of(loggedEvent.size() - 1));
    }

    @Test
    public void getFromMiddleOfSingleEvent() {
        LockWatchEvent loggedEvent = logLocks(100);

        LockWatchStateUpdate updateWithNoFromVersion = log.getLogDiff(OptionalLong.of(loggedEvent.size() - 10));
        assertThat(updateWithNoFromVersion.success()).isTrue();
        assertThat(updateWithNoFromVersion.events()).isEmpty();
        assertThat(updateWithNoFromVersion.lastKnownVersion()).isEqualTo(OptionalLong.of(loggedEvent.size() - 1));
    }

    @Test
    public void getFromAfterSingleEvent() {
        LockWatchEvent loggedEvent = logLocks(100);

        LockWatchStateUpdate updateWithNoFromVersion = log.getLogDiff(OptionalLong.of(loggedEvent.size() + 10));
        assertThat(updateWithNoFromVersion.success()).isFalse();
        assertThat(updateWithNoFromVersion.events()).isEmpty();
        assertThat(updateWithNoFromVersion.lastKnownVersion()).isEqualTo(OptionalLong.of(loggedEvent.size() - 1));
    }

    @Test
    public void getFromBeginningMultipleEvents() {
        LockWatchEvent firstEvent = logLocks(200);
        LockWatchEvent secondEvent = logLocks(100);
        LockWatchEvent thirdEvent = logLocks(300);

        LockWatchStateUpdate updateWithNoFromVersion = log.getLogDiff(OptionalLong.of(-1));
        assertThat(updateWithNoFromVersion.success()).isTrue();
        assertThat(updateWithNoFromVersion.events()).containsExactly(firstEvent, secondEvent, thirdEvent);
        assertThat(updateWithNoFromVersion.lastKnownVersion())
                .isEqualTo(OptionalLong.of(firstEvent.size() + secondEvent.size() + thirdEvent.size() - 1));
    }

    @Test
    public void getFromEndOfFirstEvent() {
        LockWatchEvent firstEvent = logLocks(200);
        LockWatchEvent secondEvent = logLocks(100);
        LockWatchEvent thirdEvent = logLocks(300);

        LockWatchStateUpdate updateWithNoFromVersion = log.getLogDiff(OptionalLong.of(firstEvent.size() - 1));
        assertThat(updateWithNoFromVersion.success()).isTrue();
        assertThat(updateWithNoFromVersion.events()).containsExactly(secondEvent, thirdEvent);
        assertThat(updateWithNoFromVersion.lastKnownVersion())
                .isEqualTo(OptionalLong.of(firstEvent.size() + secondEvent.size() + thirdEvent.size() - 1));
    }

    @Test
    public void getFromMiddleOfFirstEvent() {
        LockWatchEvent firstEvent = logLocks(200);
        LockWatchEvent secondEvent = logLocks(100);
        LockWatchEvent thirdEvent = logLocks(300);

        LockWatchStateUpdate updateWithNoFromVersion = log.getLogDiff(OptionalLong.of(firstEvent.size() - 100));
        assertThat(updateWithNoFromVersion.success()).isTrue();
        assertThat(updateWithNoFromVersion.events()).containsExactly(secondEvent, thirdEvent);
        assertThat(updateWithNoFromVersion.lastKnownVersion())
                .isEqualTo(OptionalLong.of(firstEvent.size() + secondEvent.size() + thirdEvent.size() - 1));
    }

    @Test
    public void fillUpBuffer() {
        LockWatchEvent firstEvent = logLocks(500);
        LockWatchEvent secondEvent = logLocks(400);
        LockWatchEvent thirdEvent = logLocks(500);
        LockWatchEvent fourthEvent = logLocks(100);
        int numDescriptors = firstEvent.size() + secondEvent.size() + thirdEvent.size() + fourthEvent.size();

        LockWatchStateUpdate updateWithNoFromVersion = log.getLogDiff(OptionalLong.of(numDescriptors - 950));
        assertThat(updateWithNoFromVersion.success()).isTrue();
        assertThat(updateWithNoFromVersion.events()).containsExactly(thirdEvent, fourthEvent);
        assertThat(updateWithNoFromVersion.lastKnownVersion()).isEqualTo(OptionalLong.of(numDescriptors - 1));
    }

    @Test
    public void hugeEvent() {
        LockWatchEvent event = logLocks(WINDOW_SIZE * 2);

        LockWatchStateUpdate update = log.getLogDiff(OptionalLong.of(-1));
        assertThat(update.success()).isTrue();
        assertThat(update.events()).containsExactly(event);
        assertThat(update.lastKnownVersion()).isEqualTo(OptionalLong.of(event.size() - 1));
    }

    private LockWatchEvent logLocks(int numDescriptors) {
        Set<LockDescriptor> descriptors = generateDescriptors(numDescriptors);
        LockToken token = generateToken();
        log.logLock(descriptors, token);
        LockWatchEvent event = LockEvent.builder(descriptors, token).build(nextSequence);
        nextSequence = nextSequence + numDescriptors;
        return event;
    }

    private static Set<LockDescriptor> generateDescriptors(int num) {
        return IntStream.range(0, num)
                .mapToObj(ignore -> StringLockDescriptor.of(UUID.randomUUID().toString()))
                .collect(Collectors.toSet());
    }

    private static LockToken generateToken() {
        return LockToken.of(UUID.randomUUID());
    }
}

/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.palantir.lock.LockDescriptor;
import com.palantir.lock.StringLockDescriptor;
import java.util.UUID;
import javax.ws.rs.NotSupportedException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public final class NamedMinTimestampLockTest {
    private static final UUID REQUEST_ID = UUID.randomUUID();
    private static final long TIMESTAMP = 5L;

    @Mock
    private NamedMinTimestampTracker tracker;

    private NamedMinTimestampLock lock;

    @BeforeEach
    public void before() {
        lock = NamedMinTimestampLock.create(tracker, TIMESTAMP);
    }

    @Test
    public void lockCompletesSuccessfullyInstantly() {
        assertThat(lock.lock(REQUEST_ID).isCompletedSuccessfully()).isTrue();
    }

    @Test
    public void lockCallsTrackersLock() {
        lock.lock(REQUEST_ID);
        verify(tracker).lock(TIMESTAMP, REQUEST_ID);
    }

    @Test
    public void unlockCallsTrackersUnlock() {
        lock.unlock(REQUEST_ID);
        verify(tracker).unlock(TIMESTAMP, REQUEST_ID);
    }

    @Test
    public void getDescriptorReturnsTrackersDescriptor() {
        LockDescriptor descriptor = StringLockDescriptor.of("foo");

        NamedMinTimestampTracker mockTracker = mock(NamedMinTimestampTracker.class);
        when(mockTracker.getDescriptor(TIMESTAMP)).thenReturn(descriptor);

        NamedMinTimestampLock lockWithStubbedTracker = NamedMinTimestampLock.create(mockTracker, TIMESTAMP);
        assertThat(lockWithStubbedTracker.getDescriptor()).isEqualTo(descriptor);
    }

    @Test
    public void timeoutIsUnsupported() {
        assertThatThrownBy(() -> lock.timeout(REQUEST_ID)).isExactlyInstanceOf(NotSupportedException.class);
    }

    @Test
    public void waitUntilAvailableIsUnsupported() {
        assertThatThrownBy(() -> lock.waitUntilAvailable(REQUEST_ID)).isExactlyInstanceOf(NotSupportedException.class);
    }
}

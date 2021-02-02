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
package com.palantir.atlasdb.timelock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.palantir.atlasdb.timelock.lock.AsyncLockService;
import com.palantir.atlasdb.timelock.lock.LockLog;
import com.palantir.timestamp.ManagedTimestampService;
import org.junit.Test;

public class AsyncTimelockServiceImplTest {
    @Test
    public void delegatesInitializationCheck() {
        ManagedTimestampService mockMts = mock(ManagedTimestampService.class);
        AsyncTimelockServiceImpl service =
                new AsyncTimelockServiceImpl(mock(AsyncLockService.class), mockMts, mock(LockLog.class));
        when(mockMts.isInitialized()).thenReturn(false).thenReturn(true);

        assertThat(service.isInitialized()).isFalse();
        assertThat(service.isInitialized()).isTrue();
    }
}

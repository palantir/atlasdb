/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;

import com.palantir.atlasdb.timelock.lock.AsyncLockService;
import com.palantir.atlasdb.timelock.paxos.ManagedTimestampService;

public class AsyncTimelockServiceImplTest {
    @Test
    public void delegatesInitializationCheck() {
        ManagedTimestampService mockMts = mock(ManagedTimestampService.class);
        AsyncTimelockServiceImpl service = new AsyncTimelockServiceImpl(mock(AsyncLockService.class), mockMts);
        when(mockMts.isInitialized())
                .thenReturn(false)
                .thenReturn(true);

        assertFalse(service.isInitialized());
        assertTrue(service.isInitialized());
    }
}

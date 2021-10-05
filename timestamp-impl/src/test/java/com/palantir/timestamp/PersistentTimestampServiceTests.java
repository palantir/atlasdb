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
package com.palantir.timestamp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.palantir.common.remoting.ServiceNotAvailableException;
import org.junit.Test;

// Test PersistentTimestampServiceImpl by fully instantiating it with an InMemoryTimestampBoundStore.
// See also PersistentTimestampServiceMockingTest that mocks AvailableTimestamps instead.
public class PersistentTimestampServiceTests extends AbstractTimestampServiceTests {

    private PersistentTimestampService persistentTimestampService;
    private InMemoryTimestampBoundStore timestampBoundStore;

    @Override
    protected TimestampService getTimestampService() {
        return getSingletonTimestampService();
    }

    @Override
    protected TimestampManagementService getTimestampManagementService() {
        return getSingletonTimestampService();
    }

    private PersistentTimestampService getSingletonTimestampService() {
        if (timestampBoundStore == null || persistentTimestampService == null) {
            timestampBoundStore = new InMemoryTimestampBoundStore();
            persistentTimestampService = PersistentTimestampServiceImpl.create(timestampBoundStore);
        }
        return persistentTimestampService;
    }

    @Test
    public void shouldLimitRequestsForMoreThanTenThousandTimestamps() {
        assertThat(getTimestampService().getFreshTimestamps(100_000).size()).isEqualTo(10_000L);
    }

    @Test
    public void throwsServiceNotAvailableExceptionIfThereAreMultipleServersRunning() {
        timestampBoundStore.pretendMultipleServersAreRunning();
        assertThatThrownBy(() -> getTimestampService().getFreshTimestamp())
                .isInstanceOf(ServiceNotAvailableException.class);
    }

    @Test
    public void shouldRethrowAllocationExceptions() {
        final IllegalArgumentException failure = new IllegalArgumentException();
        timestampBoundStore.failWith(failure);
        assertThatThrownBy(() -> getTimestampService().getFreshTimestamp())
                .isInstanceOf(RuntimeException.class)
                .hasCause(failure);
    }

    @Test
    public void shouldNotTryToStoreANewBoundIfMultipleServicesAreRunning() {
        timestampBoundStore.pretendMultipleServersAreRunning();

        getTimestampAndIgnoreErrors();
        getTimestampAndIgnoreErrors();

        assertThat(timestampBoundStore.numberOfAllocations()).isLessThan(2);
    }

    @Test
    public void shouldRejectFastForwardToTheSentinelValue() {
        assertThatThrownBy(() -> getTimestampManagementService()
                        .fastForwardTimestamp(TimestampManagementService.SENTINEL_TIMESTAMP))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private void getTimestampAndIgnoreErrors() {
        try {
            getTimestampService().getFreshTimestamp();
        } catch (Exception e) {
            // expected
        }
    }
}

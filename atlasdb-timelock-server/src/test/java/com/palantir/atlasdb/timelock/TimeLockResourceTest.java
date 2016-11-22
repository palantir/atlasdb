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
package com.palantir.atlasdb.timelock;

import static org.mockito.Mockito.mock;

import javax.ws.rs.NotFoundException;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.palantir.lock.LockService;
import com.palantir.timestamp.TimestampAdminService;
import com.palantir.timestamp.TimestampService;

public class TimeLockResourceTest {
    private static final String EXISTING_CLIENT = "existing-client";
    private static final String NON_EXISTING_CLIENT = "non-existing-client";

    private static final TimestampService TIME_SERVICE = mock(TimestampService.class);
    private static final LockService LOCK_SERVICE = mock(LockService.class);
    private static final TimestampAdminService ADMIN_TIME_SERVICE = mock(TimestampAdminService.class);
    private static final TimeLockServices TIME_LOCK_SERVICES = ImmutableTimeLockServices.builder()
            .timeService(TIME_SERVICE)
            .lockService(LOCK_SERVICE)
            .adminTimeService(ADMIN_TIME_SERVICE)
            .build();

    private static final TimeLockResource RESOURCE = new TimeLockResource(
            ImmutableMap.of(EXISTING_CLIENT, TIME_LOCK_SERVICES));

    @Test
    public void canGetExistingTimeService() {
        RESOURCE.getTimeService(EXISTING_CLIENT);
    }

    @Test(expected = NotFoundException.class)
    public void throwWhenTimeServiceDoesntExist() {
        RESOURCE.getTimeService(NON_EXISTING_CLIENT);
    }

    @Test
    public void canGetExistingLockService() {
        RESOURCE.getLockService(EXISTING_CLIENT);
    }

    @Test(expected = NotFoundException.class)
    public void throwWhenLockServiceDoesntExist() {
        RESOURCE.getLockService(NON_EXISTING_CLIENT);
    }

    @Test
    public void canGetExistingTimeAdminService() {
        RESOURCE.getTimeAdminService(EXISTING_CLIENT);
    }

    @Test(expected = NotFoundException.class)
    public void throwWhenTimeAdminServiceDoesntExist() {
        RESOURCE.getTimeAdminService(NON_EXISTING_CLIENT);
    }
}

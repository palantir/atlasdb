/**
 * Copyright 2017 Palantir Technologies
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigInteger;

import org.junit.Before;
import org.junit.Test;

import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.RemoteLockService;

public class LockServiceHealthCheckTests {
    private static final LockRefreshToken LOCK_REFRESH_TOKEN = new LockRefreshToken(BigInteger.ZERO, 0);
    public static final String HEALTHCHECK_LOCK = "lock-service-healthcheck";

    private RemoteLockService lockService = mock(RemoteLockService.class);

    private LockServiceHealthCheck lockServiceHealthCheck;

    @Before
    public void before() {
        lockServiceHealthCheck = new LockServiceHealthCheck(lockService);
    }

    @Test
    public void healthyIfCanTakeAndReturnLock() throws Exception {
        when(lockService.lock(eq(HEALTHCHECK_LOCK), any())).thenReturn(LOCK_REFRESH_TOKEN);
        assertThat(lockServiceHealthCheck.check().isHealthy()).isTrue();
        verify(lockService).unlock(LOCK_REFRESH_TOKEN);
    }

    @Test
    public void throwsIfCannotTakeLock() throws Exception {
        when(lockService.lock(eq(HEALTHCHECK_LOCK), any())).thenReturn(null);
        assertThatExceptionOfType(IllegalStateException.class).isThrownBy(lockServiceHealthCheck::check);
    }
}

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
package com.palantir.lock.impl;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import static uk.org.lidalia.slf4jtest.LoggingEvent.debug;
import static uk.org.lidalia.slf4jtest.LoggingEvent.info;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.palantir.common.proxy.SerializingProxy;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.LockService;
import com.palantir.lock.LockServiceTest;

import uk.org.lidalia.slf4jext.Level;
import uk.org.lidalia.slf4jtest.TestLogger;
import uk.org.lidalia.slf4jtest.TestLoggerFactory;

public final class LockServiceImplTest extends LockServiceTest {
    public static final long SLOW_LOG_TRIGGER_MILLIS = LockServiceImpl.DEBUG_SLOW_LOG_TRIGGER_MILLIS + 10;
    private LockServiceImpl lockService;
    TestLogger testSlowLogger = TestLoggerFactory.getTestLogger(SlowLockLogger.class);
    TestLogger lockServiceImplLogger = TestLoggerFactory.getTestLogger(LockServiceImpl.class);

    @Before
    public void setUp() {
        lockServiceImplLogger.setEnabledLevels(Level.INFO);
        lockService = LockServiceImpl.create(new LockServerOptions() {
            private static final long serialVersionUID = 1L;

            @Override
            public boolean isStandaloneServer() {
                return false;
            }

            @Override
            public long slowLogTriggerMillis() {
                return SLOW_LOG_TRIGGER_MILLIS;
            }
        });

    }

    @After
    public void clearLoggers() {
        TestLoggerFactory.clear();
    }

    protected LockService getLockService() {
        return SerializingProxy.newProxyInstance(LockService.class, lockService);
    }

    @Test
    public void slowLogShouldBeEnabled() {
        assertThat(lockService.isSlowLogEnabled(), is(true));
    }

    @Test
    public void slowLogShouldBeLoggedWhenLockResponseIsSlowButShouldNotBeLoggedAtDebug() {
        long lockDurationMillis = SLOW_LOG_TRIGGER_MILLIS + 5;
        lockServiceImplLogger.setEnabledLevels(Level.DEBUG);
        lockService.logSlowLockAcquisition("test_lockId", LockClient.ANONYMOUS, lockDurationMillis);

        assertThat(lockServiceImplLogger.isDebugEnabled(), is(true));
        assertThat(lockServiceImplLogger.getLoggingEvents().size(), is(0));

        assertThat(testSlowLogger.getLoggingEvents().size(), is(1));
        assertThat(Iterables.getOnlyElement(testSlowLogger.getLoggingEvents()),
                is(info("Blocked for {} ms to acquire lock {} {}.", lockDurationMillis, "test_lockId",
                        "unsuccessfully")));
    }

    @Test
    public void debugLogShouldBeLoggedIfLockResponseTimeIsLessThanSlowLogTriggerMillisButGreaterThanDebugTriggerTime() {
        lockServiceImplLogger.setEnabledLevels(Level.DEBUG);
        long lockDurationMillis = SLOW_LOG_TRIGGER_MILLIS - 5;
        lockService.logSlowLockAcquisition("test_lockId", LockClient.ANONYMOUS, lockDurationMillis);

        assertThat(lockServiceImplLogger.isDebugEnabled(), is(true));
        assertThat(lockServiceImplLogger.getLoggingEvents().size(), is(1));
        assertThat(Iterables.getOnlyElement(lockServiceImplLogger.getLoggingEvents()),
                is(debug("Blocked for {} ms to acquire lock {} {}.", lockDurationMillis, "test_lockId",
                        "unsuccessfully")));

        assertThat(testSlowLogger.getLoggingEvents().size(), is(0));
    }

    @Test
    public void debugOrSlowLogShouldNotBeLoggedWhenLockResponseIsNotSlow() {
        lockServiceImplLogger.setEnabledLevels(Level.DEBUG);
        lockService.logSlowLockAcquisition("test_lockId", LockClient.ANONYMOUS, LockServiceImpl.DEBUG_SLOW_LOG_TRIGGER_MILLIS - 5);
        assertThat(lockServiceImplLogger.getLoggingEvents().size(), is(0));
        assertThat(testSlowLogger.getLoggingEvents().size(), is(0));
    }
}

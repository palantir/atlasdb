/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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
import static uk.org.lidalia.slf4jtest.LoggingEvent.warn;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.palantir.lock.LockClient;
import com.palantir.lock.LockServerOptions;

import uk.org.lidalia.slf4jext.Level;
import uk.org.lidalia.slf4jtest.LoggingEvent;
import uk.org.lidalia.slf4jtest.TestLogger;
import uk.org.lidalia.slf4jtest.TestLoggerFactory;

public final class LockServiceImplTest {
    public static final long SLOW_LOG_TRIGGER_MILLIS = LockServiceImpl.DEBUG_SLOW_LOG_TRIGGER_MILLIS + 10;
    private static final String TEST_LOCKID = "test_lockId";
    private static LockServiceImpl lockServiceWithSlowLogEnabled;
    private static LockServiceImpl lockServiceWithSlowLogDisabled;

    private TestLogger testSlowLogger;
    private TestLogger testLockServiceImplLogger;

    @BeforeClass
    public static void setupLockService() {
        lockServiceWithSlowLogEnabled = createLockServiceWithSlowLogEnabled(true);
        lockServiceWithSlowLogDisabled = createLockServiceWithSlowLogEnabled(false);
    }

    @Before
    public void setUpLoggers() {
        testSlowLogger = TestLoggerFactory.getTestLogger(SlowLockLogger.class);
        testLockServiceImplLogger = TestLoggerFactory.getTestLogger(LockServiceImpl.class);
        testSlowLogger.setEnabledLevelsForAllThreads(Level.WARN, Level.ERROR);
        testLockServiceImplLogger.setEnabledLevelsForAllThreads(Level.DEBUG);
    }

    @After
    public void clearLoggers() {
        TestLoggerFactory.clearAll();
    }

    private static LockServiceImpl createLockServiceWithSlowLogEnabled(boolean isSlowLogEnabled) {
        return LockServiceImpl.create(LockServerOptions.builder()
                .isStandaloneServer(false)
                .slowLogTriggerMillis(isSlowLogEnabled ? SLOW_LOG_TRIGGER_MILLIS : 0)
                .build());
    }

    @Test
    public void slowLogShouldBeEnabledIfSlowLogTriggerMillisIsSetToPositiveValue() {
        assertThat(lockServiceWithSlowLogEnabled.isSlowLogEnabled(), is(true));
    }

    @Test
    public void slowLogShouldNotBeEnabledIfSlowLogTriggerMillisIsSetToZero() {
        assertThat(lockServiceWithSlowLogDisabled.isSlowLogEnabled(), is(false));
    }

    @Test
    public void slowLogShouldBeLoggedWhenLockResponseIsSlowButShouldNotBeLoggedAtDebug() {
        long lockDurationMillis = SLOW_LOG_TRIGGER_MILLIS + 5;
        lockServiceWithSlowLogEnabled.logSlowLockAcquisition(TEST_LOCKID, LockClient.ANONYMOUS, lockDurationMillis);

        assertThat(testLockServiceImplLogger.isDebugEnabled(), is(true));
        assertThat(testLockServiceImplLogger.getLoggingEvents().size(), is(0));

        assertThat(testSlowLogger.getLoggingEvents().size(), is(1));
        assertContainsMatchingLoggingEvent(testSlowLogger.getLoggingEvents(),
                warn("Blocked for {} ms to acquire lock {} {}.", lockDurationMillis, TEST_LOCKID, "unsuccessfully"));
    }

    @Test
    public void debugLogShouldBeLoggedWhenLockResponseIsSlowButSlowLogIsDisabled() {
        long lockDurationMillis = SLOW_LOG_TRIGGER_MILLIS + 5;
        lockServiceWithSlowLogDisabled.logSlowLockAcquisition(TEST_LOCKID, LockClient.ANONYMOUS, lockDurationMillis);

        assertThat(testLockServiceImplLogger.isDebugEnabled(), is(true));
        assertThat(testLockServiceImplLogger.getLoggingEvents().size(), is(1));
        assertContainsMatchingLoggingEvent(testLockServiceImplLogger.getLoggingEvents(),
                debug("Blocked for {} ms to acquire lock {} {}.", lockDurationMillis, TEST_LOCKID, "unsuccessfully"));

        assertThat(testSlowLogger.getLoggingEvents().size(), is(0));
    }

    @Test
    public void debugLogShouldBeLoggedIfLockResponseTimeIsBetweenDebugTriggerTimeAndSlowLogTriggerMillis() {
        long lockDurationMillis = SLOW_LOG_TRIGGER_MILLIS - 5;
        lockServiceWithSlowLogEnabled.logSlowLockAcquisition(TEST_LOCKID, LockClient.ANONYMOUS, lockDurationMillis);

        assertThat(testLockServiceImplLogger.isDebugEnabled(), is(true));
        assertContainsMatchingLoggingEvent(testLockServiceImplLogger.getLoggingEvents(),
                debug("Blocked for {} ms to acquire lock {} {}.", lockDurationMillis, TEST_LOCKID, "unsuccessfully"));

        assertThat(testSlowLogger.getLoggingEvents().size(), is(0));
    }

    @Test
    public void debugOrSlowLogShouldNotBeLoggedWhenLockResponseIsNotSlow() {
        lockServiceWithSlowLogEnabled.logSlowLockAcquisition(
                TEST_LOCKID, LockClient.ANONYMOUS, LockServiceImpl.DEBUG_SLOW_LOG_TRIGGER_MILLIS - 5);
        assertThat(testLockServiceImplLogger.getLoggingEvents().size(), is(0));
        assertThat(testSlowLogger.getLoggingEvents().size(), is(0));
    }

    private static void assertContainsMatchingLoggingEvent(List<LoggingEvent> actuals, LoggingEvent expected) {
        List<String> expectedParamStrings = extractArgumentsAsStringList(expected);
        assertThat(actuals.stream()
                .filter(event -> event.getLevel() == expected.getLevel()
                        && event.getMessage().equals(expected.getMessage())
                        && expectedParamStrings.equals(extractArgumentsAsStringList(event)))
                .collect(Collectors.toSet()).size(),
                is(1));
    }

    private static List<String> extractArgumentsAsStringList(LoggingEvent event) {
        return event.getArguments()
                .asList()
                .stream()
                .map(Object::toString)
                .collect(Collectors.toList());
    }
}

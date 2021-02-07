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
package com.palantir.lock.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.org.lidalia.slf4jtest.LoggingEvent.debug;
import static uk.org.lidalia.slf4jtest.LoggingEvent.warn;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSortedMap;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.SimpleTimeDuration;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.client.LockRefreshingLockService;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
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
        assertThat(lockServiceWithSlowLogEnabled.isSlowLogEnabled()).isTrue();
    }

    @Test
    public void slowLogShouldNotBeEnabledIfSlowLogTriggerMillisIsSetToZero() {
        assertThat(lockServiceWithSlowLogDisabled.isSlowLogEnabled()).isFalse();
    }

    @Test
    public void slowLogShouldBeLoggedWhenLockResponseIsSlowButShouldNotBeLoggedAtDebug() {
        long lockDurationMillis = SLOW_LOG_TRIGGER_MILLIS + 5;
        lockServiceWithSlowLogEnabled.logSlowLockAcquisition(TEST_LOCKID, LockClient.ANONYMOUS, lockDurationMillis);

        assertThat(testLockServiceImplLogger.isDebugEnabled()).isTrue();
        assertThat(testLockServiceImplLogger.getLoggingEvents()).isEmpty();

        assertThat(testSlowLogger.getLoggingEvents()).hasSize(1);
        assertContainsMatchingLoggingEvent(
                testSlowLogger.getLoggingEvents(),
                warn("Blocked for {} ms to acquire lock {} {}.", lockDurationMillis, TEST_LOCKID, "unsuccessfully"));
    }

    @Test
    public void debugLogShouldBeLoggedWhenLockResponseIsSlowButSlowLogIsDisabled() {
        long lockDurationMillis = SLOW_LOG_TRIGGER_MILLIS + 5;
        lockServiceWithSlowLogDisabled.logSlowLockAcquisition(TEST_LOCKID, LockClient.ANONYMOUS, lockDurationMillis);

        assertThat(testLockServiceImplLogger.isDebugEnabled()).isTrue();
        assertThat(testLockServiceImplLogger.getLoggingEvents()).hasSize(1);
        assertContainsMatchingLoggingEvent(
                testLockServiceImplLogger.getLoggingEvents(),
                debug("Blocked for {} ms to acquire lock {} {}.", lockDurationMillis, TEST_LOCKID, "unsuccessfully"));

        assertThat(testSlowLogger.getLoggingEvents()).isEmpty();
    }

    @Test
    public void debugLogShouldBeLoggedIfLockResponseTimeIsBetweenDebugTriggerTimeAndSlowLogTriggerMillis() {
        long lockDurationMillis = SLOW_LOG_TRIGGER_MILLIS - 5;
        lockServiceWithSlowLogEnabled.logSlowLockAcquisition(TEST_LOCKID, LockClient.ANONYMOUS, lockDurationMillis);

        assertThat(testLockServiceImplLogger.isDebugEnabled()).isTrue();
        assertContainsMatchingLoggingEvent(
                testLockServiceImplLogger.getLoggingEvents(),
                debug("Blocked for {} ms to acquire lock {} {}.", lockDurationMillis, TEST_LOCKID, "unsuccessfully"));

        assertThat(testSlowLogger.getLoggingEvents()).isEmpty();
    }

    @Test
    public void debugOrSlowLogShouldNotBeLoggedWhenLockResponseIsNotSlow() {
        lockServiceWithSlowLogEnabled.logSlowLockAcquisition(
                TEST_LOCKID, LockClient.ANONYMOUS, LockServiceImpl.DEBUG_SLOW_LOG_TRIGGER_MILLIS - 5);
        assertThat(testLockServiceImplLogger.getLoggingEvents()).isEmpty();
        assertThat(testSlowLogger.getLoggingEvents()).isEmpty();
    }

    @Test
    public void verifySerializedBatchOfLockRequestsSmallerThan45MB() throws InterruptedException, IOException {
        Set<LockRefreshToken> tokens = new HashSet<>();

        // divide batch size by 1000 and check size in KB as approximation
        for (int i = 0; i < LockRefreshingLockService.REFRESH_BATCH_SIZE / 1000; i++) {
            LockRequest request = LockRequest.builder(ImmutableSortedMap.of(
                            StringLockDescriptor.of(UUID.randomUUID().toString()), LockMode.READ))
                    .timeoutAfter(SimpleTimeDuration.of(10, TimeUnit.MILLISECONDS))
                    .build();
            tokens.add(lockServiceWithSlowLogDisabled.lock("test", request));
        }

        Assertions.assertThat(new ObjectMapper().writeValueAsString(tokens)).hasSizeLessThan(45_000);
    }

    private static void assertContainsMatchingLoggingEvent(List<LoggingEvent> actuals, LoggingEvent expected) {
        List<String> expectedParamStrings = extractArgumentsAsStringList(expected);
        assertThat(actuals.stream()
                        .filter(event -> event.getLevel() == expected.getLevel()
                                && event.getMessage().equals(expected.getMessage())
                                && expectedParamStrings.equals(extractArgumentsAsStringList(event)))
                        .collect(Collectors.toSet()))
                .hasSize(1);
    }

    private static List<String> extractArgumentsAsStringList(LoggingEvent event) {
        return event.getArguments().asList().stream().map(Object::toString).collect(Collectors.toList());
    }
}

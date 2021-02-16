/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.jepsen.lock;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.jepsen.CheckerResult;
import com.palantir.atlasdb.jepsen.events.Event;
import com.palantir.atlasdb.jepsen.utils.TestEventUtils;
import org.junit.Test;

public class LockAcquisitionLivenessCheckerTest {
    private static final long TIME = 3000L;

    @Test
    public void shouldSucceedIfLockWasSuccessful() {
        assertThat(runLockAcquisitionLivenessChecker(TestEventUtils.lockSuccess(TIME, 0)))
                .satisfies(LockAcquisitionLivenessCheckerTest::assertResultValidAndErrorFree);
    }

    @Test
    public void shouldNotSucceedIfLockWasUnsuccessful() {
        CheckerResult result = runLockAcquisitionLivenessChecker(TestEventUtils.lockFailure(TIME, 0));

        assertResultNotValidAndHasOneErrorAtTimestamp(result, TIME);
    }

    @Test
    public void shouldNotSucceedIfWeOnlyInvokedLock() {
        CheckerResult result = runLockAcquisitionLivenessChecker(TestEventUtils.invokeLock(TIME, 0, "lock"));

        assertResultNotValidAndHasOneErrorAtTimestamp(result, TIME);
    }

    @Test
    public void shouldNotSucceedIfRefreshOrUnlockWereSuccessful() {
        CheckerResult result = runLockAcquisitionLivenessChecker(
                TestEventUtils.refreshSuccess(TIME, 0), TestEventUtils.unlockSuccess(TIME + 1, 0));

        assertResultNotValidAndHasOneErrorAtTimestamp(result, TIME + 1);
    }

    @Test
    public void shouldNotSucceedOnErrorEvent() {
        CheckerResult result =
                runLockAcquisitionLivenessChecker(TestEventUtils.createFailEvent(TIME, 0, "403 403 403 403 403"));

        assertResultNotValidAndHasOneErrorAtTimestamp(result, TIME);
    }

    @Test
    public void shouldSucceedIfLocksWereAcquiredAmongOtherEvents() {
        assertThat(runLockAcquisitionLivenessChecker(
                        TestEventUtils.createFailEvent(TIME, 0, "null"),
                        TestEventUtils.invokeLock(TIME + 1, 0, "lock"),
                        TestEventUtils.createInfoEvent(TIME + 2, 0, "theData"),
                        TestEventUtils.lockSuccess(TIME + 3, 0)))
                .satisfies(LockAcquisitionLivenessCheckerTest::assertResultValidAndErrorFree);
    }

    @Test
    public void shouldFailIfNoEventsProvided() {
        assertThat(runLockAcquisitionLivenessChecker()).satisfies(result -> {
            assertThat(result.valid()).isFalse();
            assertThat(result.errors()).hasSize(1);
        });
    }

    @Test
    public void shouldFailIfNoLocksWereAcquired() {
        CheckerResult result = runLockAcquisitionLivenessChecker(
                TestEventUtils.timestampOk(TIME, 0, "42"),
                TestEventUtils.createFailEvent(TIME + 1, 0, "insufficient"),
                TestEventUtils.createFailEvent(TIME + 2, 0, "inadequate"),
                TestEventUtils.invokeLock(TIME + 3, 0),
                TestEventUtils.invokeLock(TIME + 4, 0),
                TestEventUtils.invokeLock(TIME + 5, 0),
                TestEventUtils.lockFailure(TIME + 6, 0),
                TestEventUtils.createFailEvent(TIME + 7, 0, "unsuccessful"),
                TestEventUtils.lockFailure(TIME + 8, 0),
                TestEventUtils.createFailEvent(TIME + 9, 0, "inferior"),
                TestEventUtils.lockFailure(TIME + 10, 0));

        assertResultNotValidAndHasOneErrorAtTimestamp(result, TIME + 10);
    }

    private static void assertResultValidAndErrorFree(CheckerResult result) {
        assertThat(result.valid()).isTrue();
        assertThat(result.errors()).isEmpty();
    }

    private static void assertResultNotValidAndHasOneErrorAtTimestamp(CheckerResult result, long expectedTimestamp) {
        assertThat(result.valid()).isFalse();
        assertThat(Iterables.getOnlyElement(result.errors()).time()).isEqualTo(expectedTimestamp);
    }

    private static CheckerResult runLockAcquisitionLivenessChecker(Event... events) {
        LockAcquisitionLivenessChecker livenessChecker = new LockAcquisitionLivenessChecker();
        return livenessChecker.check(ImmutableList.copyOf(events));
    }
}

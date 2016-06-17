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
package com.palantir.timestamp;

import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;

import com.palantir.common.remoting.ServiceNotAvailableException;

public class TimestampAllocationFailuresTest {
    private static final RuntimeException FAILURE = new IllegalStateException();
    private static final MultipleRunningTimestampServiceError MULTIPLE_RUNNING_SERVICES_FAILURE =
            new MultipleRunningTimestampServiceError("error");

    private final Logger log = mock(Logger.class);
    private final TimestampAllocationFailures allocationFailures = new TimestampAllocationFailures(log);

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test public void
    shouldRethrowExceptions() {
        exception.expectCause(is(FAILURE));
        exception.expect(RuntimeException.class);
        exception.expectMessage("Could not allocate more timestamps");

        allocationFailures.handle(FAILURE);
    }

    @Test public void
    shouldRethrowMultipleRunningTimestampServiceErrorsAsServiceNotAvailableExceptions() {
        exception.expect(ServiceNotAvailableException.class);
        exception.expectCause(is(MULTIPLE_RUNNING_SERVICES_FAILURE));

        allocationFailures.handle(MULTIPLE_RUNNING_SERVICES_FAILURE);
    }

    @Test public void
    shouldAllowTryingToAllocateMoreTimestampsAfterANormalRuntimeException() {
        try {
            allocationFailures.handle(FAILURE);
        } catch (Exception e) {
            // Do nothing with expected exception
        }
        allocationFailures.verifyWeShouldTryToAllocateMoreTimestamps();
    }

    @Test public void
    shouldDisallowTryingToAllocateMoreTimestampsAfterAMultipleRunningTimestampServicesFailure() {
        try {
            allocationFailures.handle(MULTIPLE_RUNNING_SERVICES_FAILURE);
        } catch (Exception e) {
            // Do nothing with expected exception
        }

        exception.expectCause(is(MULTIPLE_RUNNING_SERVICES_FAILURE));
        exception.expect(ServiceNotAvailableException.class);

        allocationFailures.verifyWeShouldTryToAllocateMoreTimestamps();
    }

    @Test public void
    shouldLogTheFirstOfATypeOfExceptionToError() {
        try {
            allocationFailures.handle(FAILURE);
        } catch (Exception e) {
            // Do nothing with expected exception
        }

        verify(log).error(anyString(), eq(FAILURE));
    }

    @Test public void
    shouldLogTheSecondOfATypeOfExceptionToInfo() {
        ignoringExceptions(() -> allocationFailures.handle(FAILURE));
        ignoringExceptions(() -> allocationFailures.handle(FAILURE));

        verify(log).info(anyString(), eq(FAILURE));
    }

    @Test public void
    shouldLog2DifferentExceptionsToError() {
        ignoringExceptions(() -> allocationFailures.handle(FAILURE));
        ignoringExceptions(() -> allocationFailures.handle(MULTIPLE_RUNNING_SERVICES_FAILURE));

        verify(log).error(anyString(), eq(FAILURE));
        verify(log).error(anyString(), eq(MULTIPLE_RUNNING_SERVICES_FAILURE));
    }

    @FunctionalInterface
    interface Action {
        void perform();
    }

    private void ignoringExceptions(Action action) {
        try {
            action.perform();
        } catch (Exception e) {
            // Do nothing with expected exception
        }
    }
}

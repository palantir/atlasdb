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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.palantir.common.remoting.ServiceNotAvailableException;
import org.junit.Test;
import org.slf4j.Logger;

public class TimestampAllocationFailuresTest {
    private static final RuntimeException FAILURE = new IllegalStateException();
    private static final ServiceNotAvailableException SERVICE_NOT_AVAILABLE_EXCEPTION =
            new ServiceNotAvailableException("exception");
    private static final MultipleRunningTimestampServiceError MULTIPLE_RUNNING_SERVICES_FAILURE =
            new MultipleRunningTimestampServiceError("error");

    @SuppressWarnings("PreferStaticLoggers") // required for mockito
    private final Logger log = mock(Logger.class);

    private final TimestampAllocationFailures allocationFailures = new TimestampAllocationFailures(log);

    @Test
    public void shouldRethrowExceptions() {
        RuntimeException response = allocationFailures.responseTo(FAILURE);

        assertThat(response.getCause()).isEqualTo(FAILURE);
        assertThat(response).isInstanceOf(RuntimeException.class);
        assertThat(response.getMessage()).contains("Could not allocate more timestamps");
    }

    @Test
    public void shouldRethrowMultipleRunningTimestampServiceErrorsAsServiceNotAvailableExceptions() {
        RuntimeException response = allocationFailures.responseTo(MULTIPLE_RUNNING_SERVICES_FAILURE);

        assertThat(response).isInstanceOf(ServiceNotAvailableException.class);
        assertThat(response.getCause()).isEqualTo(MULTIPLE_RUNNING_SERVICES_FAILURE);
    }

    @Test
    public void shouldRethrowServiceNotAvailableExceptionsWithoutWrapping() {
        RuntimeException response = allocationFailures.responseTo(SERVICE_NOT_AVAILABLE_EXCEPTION);
        assertThat(response).isInstanceOf(ServiceNotAvailableException.class);
        assertThat(response).isEqualTo(SERVICE_NOT_AVAILABLE_EXCEPTION);
    }

    @Test
    public void shouldAllowTryingToIssueMoreTimestampsAfterANormalRuntimeException() {
        ignoringExceptions(() -> allocationFailures.responseTo(FAILURE));
        allocationFailures.verifyWeShouldIssueMoreTimestamps();
    }

    @Test
    public void shouldAllowTryingToIssueMoreTimestampsAfterAServiceNotAvailableException() {
        ignoringExceptions(() -> allocationFailures.responseTo(SERVICE_NOT_AVAILABLE_EXCEPTION));
        allocationFailures.verifyWeShouldIssueMoreTimestamps();
    }

    @Test
    public void shouldDisallowTryingToIssueMoreTimestampsAfterAMultipleRunningTimestampServicesFailure() {
        ignoringExceptions(() -> allocationFailures.responseTo(MULTIPLE_RUNNING_SERVICES_FAILURE));

        assertThatThrownBy(allocationFailures::verifyWeShouldIssueMoreTimestamps)
                .isInstanceOf(ServiceNotAvailableException.class)
                .getCause()
                .isEqualTo(MULTIPLE_RUNNING_SERVICES_FAILURE);
    }

    @SuppressWarnings("Slf4jConstantLogMessage")
    @Test
    public void shouldLogTheFirstOfATypeOfExceptionToError() {
        ignoringExceptions(() -> allocationFailures.responseTo(FAILURE));

        verify(log).error(anyString(), eq(FAILURE));
    }

    @SuppressWarnings("Slf4jConstantLogMessage")
    @Test
    public void shouldLogTheSecondOfATypeOfExceptionToInfo() {
        ignoringExceptions(() -> allocationFailures.responseTo(FAILURE));
        ignoringExceptions(() -> allocationFailures.responseTo(FAILURE));

        verify(log).info(anyString(), eq(FAILURE));
    }

    @SuppressWarnings("Slf4jConstantLogMessage")
    @Test
    public void shouldLog2DifferentExceptionsToError() {
        ignoringExceptions(() -> allocationFailures.responseTo(FAILURE));
        ignoringExceptions(() -> allocationFailures.responseTo(MULTIPLE_RUNNING_SERVICES_FAILURE));

        verify(log).error(anyString(), eq(FAILURE));
        verify(log).error(anyString(), eq(MULTIPLE_RUNNING_SERVICES_FAILURE));
    }

    @FunctionalInterface
    interface Action {
        void perform();
    }

    private static void ignoringExceptions(Action action) {
        try {
            action.perform();
        } catch (Exception e) {
            // Do nothing with expected exception
        }
    }
}

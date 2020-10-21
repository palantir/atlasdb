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

import com.google.common.annotations.VisibleForTesting;
import com.palantir.common.remoting.ServiceNotAvailableException;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class TimestampAllocationFailures {

    private static final String MULTIPLE_RUNNING_TIMESTAMP_SERVICES_MESSAGE =
            "This server is no longer usable as there appears to be another timestamp server running.";

    private final Logger log;

    private volatile Throwable previousAllocationFailure;
    private volatile boolean encounteredMultipleRunningTimestamps = false;

    @VisibleForTesting
    TimestampAllocationFailures(Logger log) {
        this.log = log;
    }

    public TimestampAllocationFailures() {
        this(LoggerFactory.getLogger(TimestampAllocationFailures.class));
    }

    public RuntimeException responseTo(Throwable newFailure) {
        synchronized (this) {
            logNewFailure(newFailure);
            previousAllocationFailure = newFailure;
        }

        if (newFailure instanceof MultipleRunningTimestampServiceError) {
            encounteredMultipleRunningTimestamps = true;
            return wrapMultipleRunningTimestampServiceError(newFailure);
        }

        if (newFailure instanceof ServiceNotAvailableException) {
            return (ServiceNotAvailableException) newFailure;
        }

        return new SafeRuntimeException("Could not allocate more timestamps", newFailure);
    }

    public void verifyWeShouldIssueMoreTimestamps() {
        // perform only single volatile load on hot path
        if (encounteredMultipleRunningTimestamps) {
            throw wrapMultipleRunningTimestampServiceError(this.previousAllocationFailure);
        }
    }

    private void logNewFailure(Throwable newFailure) {
        if (isSameAsPreviousFailure(newFailure)) {
            log.info(
                    "We encountered an error while trying to allocate more timestamps. "
                            + "This is a repeat of the previous failure",
                    newFailure);
        } else {
            log.error(
                    "We encountered an error while trying to allocate more timestamps. "
                            + "If this failure repeats it will be logged at the INFO level",
                    newFailure);
        }
    }

    private boolean isSameAsPreviousFailure(Throwable newFailure) {
        // perform only single volatile load on hot path
        Throwable failure = this.previousAllocationFailure;
        return failure != null && failure.getClass().equals(newFailure.getClass());
    }

    private static ServiceNotAvailableException wrapMultipleRunningTimestampServiceError(Throwable newFailure) {
        return new ServiceNotAvailableException(MULTIPLE_RUNNING_TIMESTAMP_SERVICES_MESSAGE, newFailure);
    }
}

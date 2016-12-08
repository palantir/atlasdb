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

import javax.annotation.concurrent.GuardedBy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.common.remoting.ServiceNotAvailableException;

public class TimestampAllocationFailures {
    private static final String SERVICE_UNAVAILABLE_ERROR =
            "This server is no longer usable as there appears to be another timestamp server running.";
    private static final String MULTIPLE_RUNNING_TIMESTAMP_SERVICES_MESSAGE = "This server is no longer usable as there appears to be another timestamp server running.";
    private final Logger log;

    @GuardedBy("this")
    private Throwable previousAllocationFailure;

    public TimestampAllocationFailures(Logger log) {
        this.log = log;
    }

    public TimestampAllocationFailures() {
        this(LoggerFactory.getLogger(TimestampAllocationFailures.class));
    }


    public synchronized RuntimeException responseTo(Throwable newFailure) {
        logNewFailure(newFailure);
        previousAllocationFailure = newFailure;

        if (newFailure instanceof MultipleRunningTimestampServiceError) {
            return wrapMultipleRunningTImestampServiceError(newFailure);
        }

        return new RuntimeException("Could not allocate more timestamps", newFailure);
   }

    public synchronized void verifyWeShouldTryToAllocateMoreTimestamps() {
        if(previousAllocationFailure instanceof MultipleRunningTimestampServiceError) {
            throw wrapMultipleRunningTImestampServiceError(previousAllocationFailure);
        }
    }

    private void logNewFailure(Throwable newFailure) {
        String message = "We encountered an error while trying to allocate more timestamps. ";
        if(isSameAsPreviousFailure(newFailure)) {
            log.info(message + "This is a repeat of the previous failure", newFailure);
        } else {
            log.error(message + "If this failure repeats it will be logged at the INFO level", newFailure);
        }
    }

    private boolean isSameAsPreviousFailure(Throwable newFailure) {
        if(previousAllocationFailure == null) {
            return false;
        }

        return newFailure.getClass().equals(previousAllocationFailure.getClass());
    }

    private ServiceNotAvailableException wrapMultipleRunningTImestampServiceError(Throwable newFailure) {
        return new ServiceNotAvailableException(MULTIPLE_RUNNING_TIMESTAMP_SERVICES_MESSAGE, newFailure);
    }
}

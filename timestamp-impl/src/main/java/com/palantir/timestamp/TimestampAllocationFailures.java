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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.common.remoting.ServiceNotAvailableException;
import com.palantir.exception.PalantirInterruptedException;

public class TimestampAllocationFailures {
    private static final Logger log = LoggerFactory.getLogger(TimestampAllocationFailures.class);
    private Throwable previousAllocationFailure;

    public synchronized void handle(Throwable newFailure) {
        Throwable oldFailure = previousAllocationFailure;
        previousAllocationFailure = newFailure;

        if (newFailure instanceof MultipleRunningTimestampServiceError) {
            throw new ServiceNotAvailableException("This server is no longer valid because another is running.", newFailure);
        }

        if (newFailure != null) {
            throw new RuntimeException("failed to allocate more timestamps", newFailure);
        }

        if (Thread.currentThread().isInterrupted()) {
            throw new PalantirInterruptedException("Interrupted while waiting for timestamp allocation.");
        }

        if (oldFailure != null
                && newFailure.getClass().equals(oldFailure.getClass())) {
            // QA-75825: don't keep logging error if we keep failing to allocate.
            log.info("Throwable while allocating timestamps.", newFailure);
        } else {
            log.error("Throwable while allocating timestamps.", newFailure);
        }
    }
}

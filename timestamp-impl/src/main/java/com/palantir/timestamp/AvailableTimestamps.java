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

public class AvailableTimestamps {
    static final long ALLOCATION_BUFFER_SIZE = 1000 * 1000;

    private static final Logger log = LoggerFactory.getLogger(AvailableTimestamps.class);

    private final LastReturnedTimestamp lastReturnedTimestamp;
    private final PersistentUpperLimit upperLimit;
    private volatile Throwable previousAllocationFailure;

    public AvailableTimestamps(LastReturnedTimestamp lastReturnedTimestamp, PersistentUpperLimit upperLimit) {
        this.lastReturnedTimestamp = lastReturnedTimestamp;
        this.upperLimit = upperLimit;
    }

    public boolean contains(long newTimestamp) {
        return newTimestamp <= upperLimit.get();
    }

    public synchronized void allocateMoreTimestamps() {
        try {
            upperLimit.increaseToAtLeast(lastReturnedTimestamp.get() + ALLOCATION_BUFFER_SIZE);
        } catch(Throwable e) {
            handleAllocationFailure(e);
        }
    }

    private void handleAllocationFailure(Throwable failure) {
        if (failure instanceof MultipleRunningTimestampServiceError) {
            throw new ServiceNotAvailableException("This server is no longer valid because another is running.", failure);
        }

        if (failure != null) {
            throw new RuntimeException("failed to allocate more timestamps", failure);
        }

        if (Thread.interrupted()) {
            Thread.currentThread().interrupt();
            throw new PalantirInterruptedException("Interrupted while waiting for timestamp allocation.");
        }

        if (previousAllocationFailure != null
                && failure.getClass().equals(previousAllocationFailure.getClass())) {
            // QA-75825: don't keep logging error if we keep failing to allocate.
            log.info("Throwable while allocating timestamps.", failure);
        } else {
            log.error("Throwable while allocating timestamps.", failure);
        }

        previousAllocationFailure = failure;
    }

}

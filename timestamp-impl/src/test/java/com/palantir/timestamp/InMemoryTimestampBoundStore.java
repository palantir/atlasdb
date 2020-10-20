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

import java.util.Optional;

public class InMemoryTimestampBoundStore implements TimestampBoundStore {
    private volatile long upperLimit = 0;
    private volatile int numberOfAllocations = 0;
    private boolean shouldThrowErrorMultipleServerError = false;
    private Optional<RuntimeException> error = Optional.empty();

    @Override
    public long getUpperLimit() {
        return upperLimit;
    }

    @Override
    public void storeUpperLimit(long newLimit) throws MultipleRunningTimestampServiceError {
        if (shouldThrowErrorMultipleServerError) {
            throw new MultipleRunningTimestampServiceError("error");
        }

        if (error.isPresent()) {
            throw error.orElse(null);
        }
        numberOfAllocations++;
        upperLimit = newLimit;
    }

    public void pretendMultipleServersAreRunning() {
        this.shouldThrowErrorMultipleServerError = true;
    }

    public int numberOfAllocations() {
        return numberOfAllocations;
    }

    public void failWith(RuntimeException ex) {
        this.error = Optional.of(ex);
    }
}

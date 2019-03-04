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
import com.palantir.exception.PalantirInterruptedException;

/**
 * Wraps a {@link TimestampBoundStore} and handles any {@link MultipleRunningTimestampServiceError}s thrown by the
 * delegate. It also checks for interrupts.
 */
public class ErrorCheckingTimestampBoundStore implements TimestampBoundStore {

    private final TimestampBoundStore delegate;
    private final TimestampAllocationFailures failures;

    public ErrorCheckingTimestampBoundStore(TimestampBoundStore delegate) {
        this(delegate, new TimestampAllocationFailures());
    }

    @VisibleForTesting
    ErrorCheckingTimestampBoundStore(TimestampBoundStore delegate, TimestampAllocationFailures failures) {
        this.delegate = delegate;
        this.failures = failures;
    }

    @Override
    public long getUpperLimit() {
        return delegate.getUpperLimit();
    }

    @Override
    public synchronized void storeUpperLimit(long limit) {
        failures.verifyWeShouldIssueMoreTimestamps();
        throwIfInterrupted();

        try {
            delegate.storeUpperLimit(limit);
        } catch (Throwable t) {
            throw failures.responseTo(t);
        }
    }

    private void throwIfInterrupted() {
        if (Thread.currentThread().isInterrupted()) {
            throw new PalantirInterruptedException("Was interrupted while trying to allocate more timestamps");
        }
    }
}

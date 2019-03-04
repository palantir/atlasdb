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
package com.palantir.atlasdb.http;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.InterruptedIOException;
import java.sql.Date;
import java.time.Instant;

import org.junit.Test;

import feign.RetryableException;

public class InterruptHonoringRetryerTest {

    private final InterruptHonoringRetryer retryer = new InterruptHonoringRetryer();

    @Test
    public void doesNotRetryInterruptedIoExceptions() {
        RetryableException ex = retryable(new InterruptedIOException());

        assertThatThrownBy(() -> retryer.continueOrPropagate(ex))
                .hasCauseInstanceOf(InterruptedIOException.class);
    }

    @Test
    public void retriesOtherRetryableExceptions() {
        RetryableException ex = retryable(new RuntimeException());

        retryer.continueOrPropagate(ex);
    }

    private RetryableException retryable(Throwable ex) {
        return new RetryableException("", ex, Date.from(Instant.EPOCH));
    }

}

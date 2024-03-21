/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.api.exceptions;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import org.junit.jupiter.api.Test;

public final class SafeTransactionFailedRetriableExceptionTest {
    private static final String MESSAGE = "I am a compile time constant, and so safe for logging.";
    private static final SafeArg<String> SAFE_ARG = SafeArg.of("safe", "1001");
    private static final UnsafeArg<String> UNSAFE_ARG = UnsafeArg.of("unsafe", "7, 143");
    private static final SafeTransactionFailedRetriableException SAFE_TRANSACTION_FAILED_RETRIABLE_EXCEPTION =
            new SafeTransactionFailedRetriableException(MESSAGE, SAFE_ARG, UNSAFE_ARG);
    private static final RuntimeException OTHER_EXCEPTION = new RuntimeException("boo");

    @Test
    public void isATransactionFailedRetriableException() {
        assertThat(SAFE_TRANSACTION_FAILED_RETRIABLE_EXCEPTION).isInstanceOf(TransactionFailedRetriableException.class);
    }

    @Test
    public void logMessageDoesNotContainArgs() {
        assertThat(SAFE_TRANSACTION_FAILED_RETRIABLE_EXCEPTION.getLogMessage())
                .isEqualTo(MESSAGE)
                .doesNotContain("1001")
                .doesNotContain("7, 143");
    }

    @Test
    public void argsAreAccessible() {
        assertThat(SAFE_TRANSACTION_FAILED_RETRIABLE_EXCEPTION.getArgs())
                .containsExactlyInAnyOrder(SAFE_ARG, UNSAFE_ARG);
    }

    @Test
    public void exceptionMessageHasLogMessageAndArgs() {
        // A bit fragile, but verifying the readability is useful.
        String exceptionMessage = SAFE_TRANSACTION_FAILED_RETRIABLE_EXCEPTION.getMessage();
        assertThat(exceptionMessage)
                .as("Exception message should contain the log message")
                .contains(MESSAGE);
        assertThat(exceptionMessage)
                .as("Exception message should contain the args")
                .contains("safe=1001", "unsafe=7, 143");
    }

    @Test
    public void causePropagatedOnConstruction() {
        SafeTransactionFailedRetriableException safeTransactionFailedRetriableException =
                new SafeTransactionFailedRetriableException(MESSAGE, OTHER_EXCEPTION, SAFE_ARG, UNSAFE_ARG);
        assertThat(safeTransactionFailedRetriableException.getCause()).isEqualTo(OTHER_EXCEPTION);
    }
}

/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.qos.ratelimit;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ExecutionException;

import org.junit.Test;

import com.palantir.common.exception.AtlasDbDependencyException;

public class QosAwareThrowablesTest {
    private static final Exception RATE_LIMIT_EXCEEDED_EXCEPTION =
            new RateLimitExceededException("Stop!");
    private static final Exception ATLASDB_DEPENDENCY_EXCEPTION =
            new AtlasDbDependencyException("The TimeLock is dead, long live the TimeLock");

    @Test
    public void unwrapAndThrowRateLimitExceededOrAtlasDbDependencyExceptionCanThrowRateLimitExceededException() {
        assertThatThrownBy(() -> QosAwareThrowables.unwrapAndThrowRateLimitExceededOrAtlasDbDependencyException(
                RATE_LIMIT_EXCEEDED_EXCEPTION)).isEqualTo(RATE_LIMIT_EXCEEDED_EXCEPTION);
    }

    @Test
    public void unwrapAndThrowRateLimitExceededOrAtlasDbDependencyExceptionCanThrowAtlasDbDependencyException() {
        assertThatThrownBy(() -> QosAwareThrowables.unwrapAndThrowRateLimitExceededOrAtlasDbDependencyException(
                ATLASDB_DEPENDENCY_EXCEPTION)).isEqualTo(ATLASDB_DEPENDENCY_EXCEPTION);
    }

    @Test
    public void unwrapAndThrowRateLimitExceededOrAtlasDbDependencyExceptionThrowsWrappedRateLimitExceededExceptions() {
        assertThatThrownBy(() -> QosAwareThrowables.unwrapAndThrowRateLimitExceededOrAtlasDbDependencyException(
                new ExecutionException(RATE_LIMIT_EXCEEDED_EXCEPTION))).isEqualTo(RATE_LIMIT_EXCEEDED_EXCEPTION);
        assertThatThrownBy(() -> QosAwareThrowables.unwrapAndThrowRateLimitExceededOrAtlasDbDependencyException(
                new InvocationTargetException(RATE_LIMIT_EXCEEDED_EXCEPTION))).isEqualTo(RATE_LIMIT_EXCEEDED_EXCEPTION);
    }

    @Test
    public void unwrapAndThrowRateLimitExceededOrAtlasDbDependencyExceptionThrowsWrappedAtlasDbDependencyExceptions() {
        assertThatThrownBy(() -> QosAwareThrowables.unwrapAndThrowRateLimitExceededOrAtlasDbDependencyException(
                new ExecutionException(ATLASDB_DEPENDENCY_EXCEPTION))).isEqualTo(ATLASDB_DEPENDENCY_EXCEPTION);
        assertThatThrownBy(() -> QosAwareThrowables.unwrapAndThrowRateLimitExceededOrAtlasDbDependencyException(
                new InvocationTargetException(ATLASDB_DEPENDENCY_EXCEPTION))).isEqualTo(ATLASDB_DEPENDENCY_EXCEPTION);
    }

    @Test
    public void unwrapAndThrowRateLimitExceededOrAtlasDbDependencyExceptionWrapsRuntimeExceptions() {
        assertThatThrownBy(() -> QosAwareThrowables.unwrapAndThrowRateLimitExceededOrAtlasDbDependencyException(
                new RuntimeException("runtimeException"))).isInstanceOf(AtlasDbDependencyException.class);
    }

    @Test
    public void unwrapAndThrowRateLimitExceededOrAtlasDbDependencyExceptionWrapsCheckedExceptions() {
        assertThatThrownBy(() -> QosAwareThrowables.unwrapAndThrowRateLimitExceededOrAtlasDbDependencyException(
                new IOException("ioException"))).isInstanceOf(AtlasDbDependencyException.class);
    }
}

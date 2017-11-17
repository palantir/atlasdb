/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.common.base;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.palantir.atlasdb.qos.ratelimit.RateLimitExceededException;
import com.palantir.common.exception.AtlasDbDependencyException;

public class ThrowablesTest extends Assert {
    private static final Exception RATE_LIMIT_EXCEEDED_EXCEPTION =
            new RateLimitExceededException("Stop!");
    private static final Exception ATLASDB_DEPENDENCY_EXCEPTION =
            new AtlasDbDependencyException("The TimeLock is dead, long live the TimeLock");

    @Before
    public void setUp() throws Exception {
        NoUsefulConstructorException.noUsefulConstructorCalled = false;
    }

    @Test
    public void testRewrap() {
        try {
            throwTwoArgConstructorException();
            fail("Should not get here");
        } catch (TwoArgConstructorException e) {
            TwoArgConstructorException wrapped = Throwables.rewrap(e);
            assertEquals(e.getMessage(), wrapped.getMessage());
            assertSame(e, wrapped.getCause());
        }


        try {
            throwSQLException();
            fail("Should not get here");
        } catch (SQLException e) {
            SQLException wrapped = Throwables.rewrap(e);
            assertEquals(e.getMessage(), wrapped.getMessage());
            assertSame(e, wrapped.getCause());
        }

        try {
            throwNoUsefulConstructorException();
            fail("Should not get here");
        } catch (NoUsefulConstructorException e) {
            int sizeBefore = e.getStackTrace().length;
            NoUsefulConstructorException wrapped = Throwables.rewrap(e);
            assertSame(e, wrapped);
            int sizeAfter = e.getStackTrace().length;
            assertTrue(sizeAfter + " should be > " + sizeBefore, sizeAfter > sizeBefore);
        }
    }

    @Test
    public void unwrapAndThrowRateLimitExceededOrAtlasDbDependencyExceptionCanThrowRateLimitExceededException() {
        assertThatThrownBy(() -> Throwables.unwrapAndThrowRateLimitExceededOrAtlasDbDependencyException(
                RATE_LIMIT_EXCEEDED_EXCEPTION)).isEqualTo(RATE_LIMIT_EXCEEDED_EXCEPTION);
    }

    @Test
    public void unwrapAndThrowRateLimitExceededOrAtlasDbDependencyExceptionCanThrowAtlasDbDependencyException() {
        assertThatThrownBy(() -> Throwables.unwrapAndThrowRateLimitExceededOrAtlasDbDependencyException(
                ATLASDB_DEPENDENCY_EXCEPTION)).isEqualTo(ATLASDB_DEPENDENCY_EXCEPTION);
    }

    @Test
    public void unwrapAndThrowRateLimitExceededOrAtlasDbDependencyExceptionThrowsWrappedRateLimitExceededExceptions() {
        assertThatThrownBy(() -> Throwables.unwrapAndThrowRateLimitExceededOrAtlasDbDependencyException(
                new ExecutionException(RATE_LIMIT_EXCEEDED_EXCEPTION))).isEqualTo(RATE_LIMIT_EXCEEDED_EXCEPTION);
        assertThatThrownBy(() -> Throwables.unwrapAndThrowRateLimitExceededOrAtlasDbDependencyException(
                new InvocationTargetException(RATE_LIMIT_EXCEEDED_EXCEPTION))).isEqualTo(RATE_LIMIT_EXCEEDED_EXCEPTION);
    }

    @Test
    public void unwrapAndThrowRateLimitExceededOrAtlasDbDependencyExceptionThrowsWrappedAtlasDbDependencyExceptions() {
        assertThatThrownBy(() -> Throwables.unwrapAndThrowRateLimitExceededOrAtlasDbDependencyException(
                new ExecutionException(ATLASDB_DEPENDENCY_EXCEPTION))).isEqualTo(ATLASDB_DEPENDENCY_EXCEPTION);
        assertThatThrownBy(() -> Throwables.unwrapAndThrowRateLimitExceededOrAtlasDbDependencyException(
                new InvocationTargetException(ATLASDB_DEPENDENCY_EXCEPTION))).isEqualTo(ATLASDB_DEPENDENCY_EXCEPTION);
    }

    @Test
    public void unwrapAndThrowRateLimitExceededOrAtlasDbDependencyExceptionWrapsRuntimeExceptions() {
        assertThatThrownBy(() -> Throwables.unwrapAndThrowRateLimitExceededOrAtlasDbDependencyException(
                new RuntimeException("runtimeException"))).isInstanceOf(AtlasDbDependencyException.class);
    }

    @Test
    public void unwrapAndThrowRateLimitExceededOrAtlasDbDependencyExceptionWrapsCheckedExceptions() {
        assertThatThrownBy(() -> Throwables.unwrapAndThrowRateLimitExceededOrAtlasDbDependencyException(
                new IOException("ioException"))).isInstanceOf(AtlasDbDependencyException.class);
    }

    // only has a (string, throwable) constructor
    public void throwTwoArgConstructorException() throws TwoArgConstructorException {
        throw new TwoArgConstructorException("Told you so", new IOException("Contained"));
    }

    // only has a string constructor
    public void throwSQLException() throws SQLException {
        throw new SQLException("Told you so");
    }

    // only has a (string, throwable) constructor
    public void throwNoUsefulConstructorException() throws NoUsefulConstructorException {
        throw new NoUsefulConstructorException(null);
    }

    static class TwoArgConstructorException extends Exception {
        private static final long serialVersionUID = 1L;
        public TwoArgConstructorException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    static class NoUsefulConstructorException extends Exception {
        static private boolean noUsefulConstructorCalled;
        private static final long serialVersionUID = 1L;
        public NoUsefulConstructorException(@SuppressWarnings("unused") Void never) {
            if (noUsefulConstructorCalled) {
                fail("This constructor should not be run multiple times");
            } else {
                noUsefulConstructorCalled = true;
            }
        }
    }
}

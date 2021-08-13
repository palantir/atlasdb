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
package com.palantir.common.base;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

import com.google.errorprone.annotations.CompileTimeConstant;
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.SafeLoggable;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.exceptions.SafeExceptions;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

public class ThrowablesTest {
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
            assertThat(wrapped.getMessage()).isEqualTo(e.getMessage());
            assertThat(wrapped.getCause()).isSameAs(e);
        }

        try {
            throwSQLException();
            fail("Should not get here");
        } catch (SQLException e) {
            SQLException wrapped = Throwables.rewrap(e);
            assertThat(wrapped.getMessage()).isEqualTo(e.getMessage());
            assertThat(wrapped.getCause()).isSameAs(e);
        }

        try {
            throwNoUsefulConstructorException();
            fail("Should not get here");
        } catch (NoUsefulConstructorException e) {
            int sizeBefore = e.getStackTrace().length;
            NoUsefulConstructorException wrapped = Throwables.rewrap(e);
            assertThat(wrapped).isSameAs(e);
            int sizeAfter = e.getStackTrace().length;
            assertThat(sizeBefore).isEqualTo(sizeAfter);
        }
    }

    @Test
    public void testThrowCauseAsUnchecked() {
        IOException checkedException = new IOException("I am inside the box");
        assertThatThrownBy(() -> {
                    throw Throwables.throwCauseAsUnchecked(
                            new TwoArgConstructorException("I have two args", checkedException));
                })
                .as("checked causes are wrapped in a runtime exception")
                .isInstanceOf(RuntimeException.class)
                .hasRootCause(checkedException);

        assertThatThrownBy(() -> {
                    throw Throwables.throwCauseAsUnchecked(new RuntimeException());
                })
                .as("exceptions without causes should not be allowed")
                .isInstanceOf(SafeIllegalStateException.class)
                .hasMessage("Exceptions passed to throwCauseAsUnchecked should have a cause");

        RuntimeException uncheckedException = new RuntimeException("I only make noise at runtime");
        assertThatThrownBy(() -> {
                    throw Throwables.throwCauseAsUnchecked(
                            new TwoArgConstructorException("I do not have three args", uncheckedException));
                })
                .as("unchecked causes should be propagated as-is")
                .isEqualTo(uncheckedException);
    }

    @Test
    public void testDoesNotLeakUnsafeMessage() {
        SafeRewrappableException original =
                new SafeRewrappableException("alpha", null, UnsafeArg.of("property", "secret"));
        assertThat(original.getMessage()).contains("secret");

        SafeRewrappableException rewrapped1 = Throwables.rewrap(original);
        assertThat(rewrapped1.getLogMessage()).doesNotContain("secret");

        SafeRewrappableException rewrapped2 = null;
        try {
            Throwables.rewrapAndThrowIfInstance(rewrapped1, SafeRewrappableException.class);
        } catch (SafeRewrappableException ex) {
            rewrapped2 = ex;
        }
        assertThat(rewrapped2).isNotNull();
        assertThat(rewrapped2.getLogMessage()).doesNotContain("secret");
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
        private static boolean noUsefulConstructorCalled;
        private static final long serialVersionUID = 1L;

        public NoUsefulConstructorException(@SuppressWarnings("unused") Void never) {
            if (noUsefulConstructorCalled) {
                fail("This constructor should not be run multiple times");
            } else {
                noUsefulConstructorCalled = true;
            }
        }
    }

    static class SafeRewrappableException extends RuntimeException implements SafeLoggable {
        private final String logMessage;
        private final List<Arg<?>> arguments;

        public SafeRewrappableException(@CompileTimeConstant String message, Throwable cause) {
            super(SafeExceptions.renderMessage(message), cause);
            this.logMessage = message;
            this.arguments = Collections.emptyList();
        }

        public SafeRewrappableException(@CompileTimeConstant String message, Throwable cause, Arg<?>... arguments) {
            super(SafeExceptions.renderMessage(message, arguments), cause);
            this.logMessage = message;
            this.arguments = Collections.unmodifiableList(Arrays.asList(arguments));
        }

        @Override
        public String getLogMessage() {
            return logMessage;
        }

        @Override
        public List<Arg<?>> getArgs() {
            return arguments;
        }
    }
}

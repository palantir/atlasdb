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
package com.palantir.atlasdb.keyvalue.cassandra;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import java.net.SocketTimeoutException;
import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.transport.TTransportException;
import org.junit.Before;
import org.junit.Test;

public class CassandraRequestExceptionHandlerTest {
    private static final String MESSAGE = "a exception";
    private static final Exception CAUSE = new Exception();
    private static final CassandraKeyValueServiceConfig config = mock(CassandraKeyValueServiceConfig.class);
    private static final int MAX_RETRIES_PER_HOST = 3;
    private static final int MAX_RETRIES_TOTAL = 6;

    private static final Set<Exception> CONNECTION_EXCEPTIONS = ImmutableSet.of(new SocketTimeoutException(MESSAGE),
            new CassandraClientFactory.ClientCreationFailedException(MESSAGE, CAUSE));
    private static final Set<Exception> TRANSIENT_EXCEPTIONS = ImmutableSet.of(new TTransportException());
    private static final Set<Exception> INDICATIVE_OF_CASSANDRA_LOAD_EXCEPTIONS =
            ImmutableSet.of(
                    new NoSuchElementException(),
                    new TimedOutException(),
                    new UnavailableException(),
                    new InsufficientConsistencyException(MESSAGE));
    private static final Set<Exception> FAST_FAILOVER_EXCEPTIONS = ImmutableSet.of(new InvalidRequestException());
    private static final Set<Exception> ALL_EXCEPTIONS = Stream.of(
            CONNECTION_EXCEPTIONS,
            TRANSIENT_EXCEPTIONS,
            INDICATIVE_OF_CASSANDRA_LOAD_EXCEPTIONS,
            FAST_FAILOVER_EXCEPTIONS)
            .flatMap(Collection::stream)
            .collect(Collectors.toSet());

    private static final Set<Exception> NOT_IMPLICATING_NODES_EXCEPTIONS = ImmutableSet.of(
            new InsufficientConsistencyException(MESSAGE),
            new NoSuchElementException());

    private boolean currentMode = true;
    private CassandraRequestExceptionHandler handlerLegacy;
    private CassandraRequestExceptionHandler handlerConservative;

    @Before
    public void setup() {
        handlerLegacy = new CassandraRequestExceptionHandler(
                () -> MAX_RETRIES_PER_HOST,
                () -> MAX_RETRIES_TOTAL,
                () -> false,
                new Blacklist(config));

        handlerConservative = new CassandraRequestExceptionHandler(
                () -> MAX_RETRIES_PER_HOST,
                () -> MAX_RETRIES_TOTAL,
                () -> true,
                new Blacklist(config));
    }

    @Test
    public void retryableExceptionsAreRetryableDefault() {
        for (Exception ex : ALL_EXCEPTIONS) {
            assertTrue(String.format("Exception %s should be retryable", ex), handlerLegacy.isRetryable(ex));
        }
        assertFalse("RuntimeException is not retryable", handlerLegacy.isRetryable(new RuntimeException()));
    }

    @Test
    public void connectionExceptionsWithSufficientAttemptsShouldBlacklistDefault() {
        for (Exception ex : CONNECTION_EXCEPTIONS) {
            assertFalse("MAX_RETRIES_PER_HOST - 1 attempts should not blacklist",
                    handlerLegacy.shouldBlacklist(ex, MAX_RETRIES_PER_HOST - 1));
        }

        for (Exception ex : CONNECTION_EXCEPTIONS) {
            assertTrue(String.format("MAX_RETRIES_PER_HOST attempts with exception %s should blacklist", ex),
                    handlerLegacy.shouldBlacklist(ex, MAX_RETRIES_PER_HOST));
        }

        Exception ffException = Iterables.get(FAST_FAILOVER_EXCEPTIONS, 0);
        assertFalse(String.format("Exception %s should not blacklist", ffException),
                handlerLegacy.shouldBlacklist(ffException, MAX_RETRIES_PER_HOST));
    }

    @Test
    public void connectionExceptionsShouldBackoffDefault() {
        for (Exception ex : CONNECTION_EXCEPTIONS) {
            assertTrue(String.format("Exception %s should backoff", ex),
                    handlerLegacy.shouldBackoff(ex, handlerLegacy.getStrategy()));
        }
    }

    @Test
    public void cassandraLoadExceptionsShouldBackoffDefault() {
        for (Exception ex : INDICATIVE_OF_CASSANDRA_LOAD_EXCEPTIONS) {
            assertTrue(String.format("Exception %s should backoff", ex),
                    handlerLegacy.shouldBackoff(ex, handlerLegacy.getStrategy()));
        }
    }

    @Test
    public void transientExceptionsShouldNotBackoffDefault() {
        for (Exception ex : TRANSIENT_EXCEPTIONS) {
            assertFalse(handlerLegacy.shouldBackoff(ex, handlerLegacy.getStrategy()));
        }
    }

    @Test
    public void fastFailoverExceptionsShouldNotBackoffDefault() {
        for (Exception ex : FAST_FAILOVER_EXCEPTIONS) {
            assertFalse(String.format("Exception %s should not backoff", ex),
                    handlerLegacy.shouldBackoff(ex, handlerLegacy.getStrategy()));
        }
    }

    @Test
    public void connectionExceptionRetriesOnDifferentHostAfterSufficientRetriesDefault() {
        for (Exception ex : CONNECTION_EXCEPTIONS) {
            assertFalse(String.format("Exception %s should not retry on different host", ex),
                    handlerLegacy.shouldRetryOnDifferentHost(ex, MAX_RETRIES_PER_HOST - 1,
                            handlerLegacy.getStrategy()));
        }

        for (Exception ex : CONNECTION_EXCEPTIONS) {
            assertTrue(String.format("Exception %s should retry on different host", ex),
                    handlerLegacy.shouldRetryOnDifferentHost(ex, MAX_RETRIES_PER_HOST, handlerLegacy.getStrategy()));
        }
    }

    @Test
    public void cassandraLoadExceptionRetriesOnSameHostDefault() {
        for (Exception ex : INDICATIVE_OF_CASSANDRA_LOAD_EXCEPTIONS) {
            assertFalse(handlerLegacy.shouldRetryOnDifferentHost(ex, 0, handlerLegacy.getStrategy()));
        }
    }

    @Test
    public void cassandraLoadExceptionRetriesOnDifferentHostAfterSufficientRetriesDefault() {
        for (Exception ex : INDICATIVE_OF_CASSANDRA_LOAD_EXCEPTIONS) {
            assertTrue(String.format("Exception %s should retry on different host", ex),
                    handlerLegacy.shouldRetryOnDifferentHost(ex, MAX_RETRIES_PER_HOST, handlerLegacy.getStrategy()));
        }
    }

    @Test
    public void fastFailoverExceptionAlwaysRetriesOnDifferentHostDefault() {
        for (Exception ex : FAST_FAILOVER_EXCEPTIONS) {
            assertTrue(String.format("Fast failover exception %s should always retry on different host", ex),
                    handlerLegacy.shouldRetryOnDifferentHost(ex, 0, handlerLegacy.getStrategy()));
        }
    }

    @Test
    public void retryableExceptionsAreRetryableConservative() {
        for (Exception ex : ALL_EXCEPTIONS) {
            assertTrue(String.format("Exception %s should be retryable", ex), handlerConservative.isRetryable(ex));
        }
        assertFalse("RuntimeException is not retryable", handlerConservative.isRetryable(new RuntimeException()));
    }

    @Test
    public void connectionExceptionsWithSufficientAttemptsShouldBlacklistConservative() {
        for (Exception ex : CONNECTION_EXCEPTIONS) {
            assertFalse("MAX_RETRIES_PER_HOST - 1 attempts should not blacklist",
                    handlerConservative.shouldBlacklist(ex, MAX_RETRIES_PER_HOST - 1));
        }

        for (Exception ex : CONNECTION_EXCEPTIONS) {
            assertTrue(String.format("MAX_RETRIES_PER_HOST attempts with exception %s should blacklist", ex),
                    handlerConservative.shouldBlacklist(ex, MAX_RETRIES_PER_HOST));
        }

        Exception ffException = Iterables.get(FAST_FAILOVER_EXCEPTIONS, 0);
        assertFalse(String.format("Exception %s should not blacklist", ffException),
                handlerConservative.shouldBlacklist(ffException, MAX_RETRIES_PER_HOST));
    }

    @Test
    public void connectionExceptionsShouldBackoffConservative() {
        for (Exception ex : CONNECTION_EXCEPTIONS) {
            assertTrue(String.format("Exception %s should backoff", ex),
                    handlerConservative.shouldBackoff(ex, handlerConservative.getStrategy()));
        }
    }

    @Test
    public void cassandraLoadExceptionsShouldBackoffConservative() {
        for (Exception ex : INDICATIVE_OF_CASSANDRA_LOAD_EXCEPTIONS) {
            assertTrue(String.format("Exception %s should backoff", ex),
                    handlerConservative.shouldBackoff(ex, handlerConservative.getStrategy()));
        }
    }

    @Test
    public void transientExceptionsShouldBackoffConservative() {
        for (Exception ex : TRANSIENT_EXCEPTIONS) {
            assertTrue(handlerConservative.shouldBackoff(ex, handlerConservative.getStrategy()));
        }
    }

    @Test
    public void fastFailoverExceptionsShouldNotBackoffConservative() {
        for (Exception ex : FAST_FAILOVER_EXCEPTIONS) {
            assertFalse(String.format("Exception %s should not backoff", ex),
                    handlerConservative.shouldBackoff(ex, handlerConservative.getStrategy()));
        }
    }

    @Test
    public void connectionExceptionRetriesOnDifferentHostAfterSufficientRetriesConservative() {
        for (Exception ex : CONNECTION_EXCEPTIONS) {
            assertFalse(String.format("Exception %s should not retry on different host", ex),
                    handlerConservative.shouldRetryOnDifferentHost(ex, MAX_RETRIES_PER_HOST - 1,
                            handlerConservative.getStrategy()));
        }

        for (Exception ex : CONNECTION_EXCEPTIONS) {
            assertTrue(String.format("Exception %s should retry on different host", ex),
                    handlerConservative.shouldRetryOnDifferentHost(ex, MAX_RETRIES_PER_HOST,
                            handlerConservative.getStrategy()));
        }
    }

    @Test
    public void cassandraLoadExceptionRetriesOnDifferentHostWithoutRetryingConservative() {
        for (Exception ex : INDICATIVE_OF_CASSANDRA_LOAD_EXCEPTIONS) {
            assertTrue(handlerConservative.shouldRetryOnDifferentHost(ex, 0, handlerConservative.getStrategy()));
        }
    }

    @Test
    public void cassandraLoadExceptionRetriesOnDifferentHostAfterSufficientRetriesConservative() {
        for (Exception ex : INDICATIVE_OF_CASSANDRA_LOAD_EXCEPTIONS) {
            assertTrue(String.format("Exception %s should retry on different host", ex),
                    handlerConservative.shouldRetryOnDifferentHost(ex, MAX_RETRIES_PER_HOST,
                            handlerConservative.getStrategy()));
        }
    }

    @Test
    public void fastFailoverExceptionAlwaysRetriesOnDifferentHostConservative() {
        for (Exception ex : FAST_FAILOVER_EXCEPTIONS) {
            assertTrue(String.format("Fast failover exception %s should always retry on different host", ex),
                    handlerConservative.shouldRetryOnDifferentHost(ex, 0, handlerConservative.getStrategy()));
        }
    }

    @Test
    public void changingHandlerModeHasNoEffectWithoutGetStrategy() {
        Exception ex = Iterables.get(TRANSIENT_EXCEPTIONS, 0);
        CassandraRequestExceptionHandler handler = new CassandraRequestExceptionHandler(
                () -> MAX_RETRIES_PER_HOST,
                () -> MAX_RETRIES_TOTAL,
                this::mutableMode,
                new Blacklist(config));
        CassandraRequestExceptionHandler.RequestExceptionHandlerStrategy conservativeStrategy = handler.getStrategy();
        assertTrue(handler.shouldBackoff(ex, handler.getStrategy()));

        flipMode();
        assertTrue(handler.shouldBackoff(ex, conservativeStrategy));
        assertFalse(handler.shouldBackoff(ex, handler.getStrategy()));
    }

    @Test
    public void exceptionAfterMaxRetriesPerHostAlwaysRetriesOnDifferentHostConservative() {
        for (Exception ex : ALL_EXCEPTIONS) {
            assertTrue(String.format("If the max retries per host has been exceeded, we should always retry on a"
                            + " different host - but we didn't for exception %s", ex),
                    handlerConservative.shouldRetryOnDifferentHost(
                            ex, MAX_RETRIES_PER_HOST + 1, handlerConservative.getStrategy()));
        }
    }

    @Test
    public void exceptionAfterMaxRetriesPerHostAlwaysRetriesOnDifferentHostDefault() {
        for (Exception ex : ALL_EXCEPTIONS) {
            assertTrue(String.format("If the max retries per host has been exceeded, we should always retry on a"
                            + " different host - but we didn't for exception %s", ex),
                    handlerConservative.shouldRetryOnDifferentHost(
                            ex, MAX_RETRIES_PER_HOST + 1, handlerLegacy.getStrategy()));
        }
    }

    @Test
    public void nonImplicatingExceptionsShouldNeverBlacklist() {
        NOT_IMPLICATING_NODES_EXCEPTIONS.forEach(ex -> {
            assertFalse(handlerConservative.shouldBlacklist(ex, MAX_RETRIES_PER_HOST + 1));
            assertFalse(handlerLegacy.shouldBlacklist(ex, MAX_RETRIES_PER_HOST + 1));
        });
    }

    @Test
    public void nonImplicatingExceptionWithConnectionAsCauseShouldNeverBlacklist() {
        Exception ex = new InsufficientConsistencyException("insufficient consistency",
                new SocketTimeoutException());
        assertFalse(handlerConservative.shouldBlacklist(ex, MAX_RETRIES_PER_HOST + 1));
        assertFalse(handlerLegacy.shouldBlacklist(ex, MAX_RETRIES_PER_HOST + 1));
    }

    private boolean mutableMode() {
        return currentMode;
    }

    private void flipMode() {
        currentMode = !currentMode;
    }
}

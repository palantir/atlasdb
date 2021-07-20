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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import java.net.SocketTimeoutException;
import java.util.Collection;
import java.util.NoSuchElementException;
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

    private static final ImmutableSet<Exception> CONNECTION_EXCEPTIONS = ImmutableSet.of(
            new SocketTimeoutException(MESSAGE),
            new CassandraClientFactory.ClientCreationFailedException(MESSAGE, CAUSE));
    private static final ImmutableSet<Exception> TRANSIENT_EXCEPTIONS = ImmutableSet.of(new TTransportException());
    private static final ImmutableSet<Exception> INDICATIVE_OF_CASSANDRA_LOAD_EXCEPTIONS = ImmutableSet.of(
            new NoSuchElementException(),
            new TimedOutException(),
            new UnavailableException(),
            new InsufficientConsistencyException(MESSAGE));
    private static final ImmutableSet<Exception> FAST_FAILOVER_EXCEPTIONS =
            ImmutableSet.of(new InvalidRequestException());
    private static final ImmutableSet<Exception> ALL_EXCEPTIONS = Stream.of(
                    CONNECTION_EXCEPTIONS,
                    TRANSIENT_EXCEPTIONS,
                    INDICATIVE_OF_CASSANDRA_LOAD_EXCEPTIONS,
                    FAST_FAILOVER_EXCEPTIONS)
            .flatMap(Collection::stream)
            .collect(ImmutableSet.toImmutableSet());

    private static final ImmutableSet<Exception> NOT_IMPLICATING_NODES_EXCEPTIONS =
            ImmutableSet.of(new InsufficientConsistencyException(MESSAGE), new NoSuchElementException());

    private boolean currentMode = true;
    private CassandraRequestExceptionHandler handlerLegacy;
    private CassandraRequestExceptionHandler handlerConservative;

    @Before
    public void setup() {
        handlerLegacy = new CassandraRequestExceptionHandler(
                () -> MAX_RETRIES_PER_HOST, () -> MAX_RETRIES_TOTAL, () -> false, new Denylist(config));

        handlerConservative = new CassandraRequestExceptionHandler(
                () -> MAX_RETRIES_PER_HOST, () -> MAX_RETRIES_TOTAL, () -> true, new Denylist(config));
    }

    @Test
    public void retryableExceptionsAreRetryableDefault() {
        for (Exception ex : ALL_EXCEPTIONS) {
            assertThat(handlerLegacy.isRetryable(ex))
                    .describedAs("Exception %s should be retryable", ex)
                    .isTrue();
        }
        assertThat(handlerLegacy.isRetryable(new RuntimeException()))
                .describedAs("RuntimeException is not retryable")
                .isFalse();
    }

    @Test
    public void connectionExceptionsWithSufficientAttemptsShouldBlacklistDefault() {
        for (Exception ex : CONNECTION_EXCEPTIONS) {
            assertThat(handlerLegacy.shouldDenylist(ex, MAX_RETRIES_PER_HOST - 1))
                    .describedAs("MAX_RETRIES_PER_HOST - 1 attempts should not blacklist")
                    .isFalse();
        }

        for (Exception ex : CONNECTION_EXCEPTIONS) {
            assertThat(handlerLegacy.shouldDenylist(ex, MAX_RETRIES_PER_HOST))
                    .describedAs("MAX_RETRIES_PER_HOST attempts with exception %s should blacklist", ex)
                    .isTrue();
        }

        Exception ffException = Iterables.get(FAST_FAILOVER_EXCEPTIONS, 0);
        assertThat(handlerLegacy.shouldDenylist(ffException, MAX_RETRIES_PER_HOST))
                .describedAs("Exception %s should not blacklist", ffException)
                .isFalse();
    }

    @Test
    public void connectionExceptionsShouldBackoffDefault() {
        for (Exception ex : CONNECTION_EXCEPTIONS) {
            assertThat(handlerLegacy.shouldBackoff(ex, handlerLegacy.getStrategy()))
                    .describedAs("Exception %s should backoff", ex)
                    .isTrue();
        }
    }

    @Test
    public void cassandraLoadExceptionsShouldBackoffDefault() {
        for (Exception ex : INDICATIVE_OF_CASSANDRA_LOAD_EXCEPTIONS) {
            assertThat(handlerLegacy.shouldBackoff(ex, handlerLegacy.getStrategy()))
                    .describedAs("Exception %s should backoff", ex)
                    .isTrue();
        }
    }

    @Test
    public void transientExceptionsShouldNotBackoffDefault() {
        for (Exception ex : TRANSIENT_EXCEPTIONS) {
            assertThat(handlerLegacy.shouldBackoff(ex, handlerLegacy.getStrategy()))
                    .isFalse();
        }
    }

    @Test
    public void fastFailoverExceptionsShouldNotBackoffDefault() {
        for (Exception ex : FAST_FAILOVER_EXCEPTIONS) {
            assertThat(handlerLegacy.shouldBackoff(ex, handlerLegacy.getStrategy()))
                    .describedAs("Exception %s should not backoff", ex)
                    .isFalse();
        }
    }

    @Test
    public void connectionExceptionRetriesOnDifferentHostAfterSufficientRetriesDefault() {
        for (Exception ex : CONNECTION_EXCEPTIONS) {
            assertThat(handlerLegacy.shouldRetryOnDifferentHost(
                            ex, MAX_RETRIES_PER_HOST - 1, handlerLegacy.getStrategy()))
                    .describedAs("Exception %s should not retry on different host", ex)
                    .isFalse();
        }

        for (Exception ex : CONNECTION_EXCEPTIONS) {
            assertThat(handlerLegacy.shouldRetryOnDifferentHost(ex, MAX_RETRIES_PER_HOST, handlerLegacy.getStrategy()))
                    .describedAs("Exception %s should retry on different host", ex)
                    .isTrue();
        }
    }

    @Test
    public void cassandraLoadExceptionRetriesOnSameHostDefault() {
        for (Exception ex : INDICATIVE_OF_CASSANDRA_LOAD_EXCEPTIONS) {
            assertThat(handlerLegacy.shouldRetryOnDifferentHost(ex, 0, handlerLegacy.getStrategy()))
                    .isFalse();
        }
    }

    @Test
    public void cassandraLoadExceptionRetriesOnDifferentHostAfterSufficientRetriesDefault() {
        for (Exception ex : INDICATIVE_OF_CASSANDRA_LOAD_EXCEPTIONS) {
            assertThat(handlerLegacy.shouldRetryOnDifferentHost(ex, MAX_RETRIES_PER_HOST, handlerLegacy.getStrategy()))
                    .describedAs("Exception %s should retry on different host", ex)
                    .isTrue();
        }
    }

    @Test
    public void fastFailoverExceptionAlwaysRetriesOnDifferentHostDefault() {
        for (Exception ex : FAST_FAILOVER_EXCEPTIONS) {
            assertThat(handlerLegacy.shouldRetryOnDifferentHost(ex, 0, handlerLegacy.getStrategy()))
                    .describedAs("Fast failover exception %s should always retry on different host", ex)
                    .isTrue();
        }
    }

    @Test
    public void retryableExceptionsAreRetryableConservative() {
        for (Exception ex : ALL_EXCEPTIONS) {
            assertThat(handlerConservative.isRetryable(ex))
                    .describedAs("Exception %s should be retryable", ex)
                    .isTrue();
        }
        assertThat(handlerConservative.isRetryable(new RuntimeException()))
                .describedAs("RuntimeException is not retryable")
                .isFalse();
    }

    @Test
    public void connectionExceptionsWithSufficientAttemptsShouldBlacklistConservative() {
        for (Exception ex : CONNECTION_EXCEPTIONS) {
            assertThat(handlerConservative.shouldDenylist(ex, MAX_RETRIES_PER_HOST - 1))
                    .describedAs("MAX_RETRIES_PER_HOST - 1 attempts should not blacklist")
                    .isFalse();
        }

        for (Exception ex : CONNECTION_EXCEPTIONS) {
            assertThat(handlerConservative.shouldDenylist(ex, MAX_RETRIES_PER_HOST))
                    .describedAs("MAX_RETRIES_PER_HOST attempts with exception %s should blacklist", ex)
                    .isTrue();
        }

        Exception ffException = Iterables.get(FAST_FAILOVER_EXCEPTIONS, 0);
        assertThat(handlerConservative.shouldDenylist(ffException, MAX_RETRIES_PER_HOST))
                .describedAs("Exception %s should not blacklist", ffException)
                .isFalse();
    }

    @Test
    public void connectionExceptionsShouldBackoffConservative() {
        for (Exception ex : CONNECTION_EXCEPTIONS) {
            assertThat(handlerConservative.shouldBackoff(ex, handlerConservative.getStrategy()))
                    .describedAs("Exception %s should backoff", ex)
                    .isTrue();
        }
    }

    @Test
    public void cassandraLoadExceptionsShouldBackoffConservative() {
        for (Exception ex : INDICATIVE_OF_CASSANDRA_LOAD_EXCEPTIONS) {
            assertThat(handlerConservative.shouldBackoff(ex, handlerConservative.getStrategy()))
                    .describedAs("Exception %s should backoff", ex)
                    .isTrue();
        }
    }

    @Test
    public void transientExceptionsShouldBackoffConservative() {
        for (Exception ex : TRANSIENT_EXCEPTIONS) {
            assertThat(handlerConservative.shouldBackoff(ex, handlerConservative.getStrategy()))
                    .isTrue();
        }
    }

    @Test
    public void fastFailoverExceptionsShouldNotBackoffConservative() {
        for (Exception ex : FAST_FAILOVER_EXCEPTIONS) {
            assertThat(handlerConservative.shouldBackoff(ex, handlerConservative.getStrategy()))
                    .describedAs("Exception %s should not backoff", ex)
                    .isFalse();
        }
    }

    @Test
    public void connectionExceptionRetriesOnDifferentHostAfterSufficientRetriesConservative() {
        for (Exception ex : CONNECTION_EXCEPTIONS) {
            assertThat(handlerConservative.shouldRetryOnDifferentHost(
                            ex, MAX_RETRIES_PER_HOST - 1, handlerConservative.getStrategy()))
                    .describedAs("Exception %s should not retry on different host", ex)
                    .isFalse();
        }

        for (Exception ex : CONNECTION_EXCEPTIONS) {
            assertThat(handlerConservative.shouldRetryOnDifferentHost(
                            ex, MAX_RETRIES_PER_HOST, handlerConservative.getStrategy()))
                    .describedAs("Exception %s should retry on different host", ex)
                    .isTrue();
        }
    }

    @Test
    public void cassandraLoadExceptionRetriesOnDifferentHostWithoutRetryingConservative() {
        for (Exception ex : INDICATIVE_OF_CASSANDRA_LOAD_EXCEPTIONS) {
            assertThat(handlerConservative.shouldRetryOnDifferentHost(ex, 0, handlerConservative.getStrategy()))
                    .isTrue();
        }
    }

    @Test
    public void cassandraLoadExceptionRetriesOnDifferentHostAfterSufficientRetriesConservative() {
        for (Exception ex : INDICATIVE_OF_CASSANDRA_LOAD_EXCEPTIONS) {
            assertThat(handlerConservative.shouldRetryOnDifferentHost(
                            ex, MAX_RETRIES_PER_HOST, handlerConservative.getStrategy()))
                    .describedAs("Exception %s should retry on different host", ex)
                    .isTrue();
        }
    }

    @Test
    public void fastFailoverExceptionAlwaysRetriesOnDifferentHostConservative() {
        for (Exception ex : FAST_FAILOVER_EXCEPTIONS) {
            assertThat(handlerConservative.shouldRetryOnDifferentHost(ex, 0, handlerConservative.getStrategy()))
                    .describedAs("Fast failover exception %s should always retry on different host", ex)
                    .isTrue();
        }
    }

    @Test
    public void changingHandlerModeHasNoEffectWithoutGetStrategy() {
        Exception ex = Iterables.get(TRANSIENT_EXCEPTIONS, 0);
        CassandraRequestExceptionHandler handler = new CassandraRequestExceptionHandler(
                () -> MAX_RETRIES_PER_HOST, () -> MAX_RETRIES_TOTAL, this::mutableMode, new Denylist(config));
        CassandraRequestExceptionHandler.RequestExceptionHandlerStrategy conservativeStrategy = handler.getStrategy();
        assertThat(handler.shouldBackoff(ex, handler.getStrategy())).isTrue();

        flipMode();
        assertThat(handler.shouldBackoff(ex, conservativeStrategy)).isTrue();
        assertThat(handler.shouldBackoff(ex, handler.getStrategy())).isFalse();
    }

    @Test
    public void exceptionAfterMaxRetriesPerHostAlwaysRetriesOnDifferentHostConservative() {
        for (Exception ex : ALL_EXCEPTIONS) {
            assertThat(handlerConservative.shouldRetryOnDifferentHost(
                            ex, MAX_RETRIES_PER_HOST + 1, handlerConservative.getStrategy()))
                    .describedAs(
                            "If the max retries per host has been exceeded, we should always retry on a"
                                    + " different host - but we didn't for exception %s",
                            ex)
                    .isTrue();
        }
    }

    @Test
    public void exceptionAfterMaxRetriesPerHostAlwaysRetriesOnDifferentHostDefault() {
        for (Exception ex : ALL_EXCEPTIONS) {
            assertThat(handlerConservative.shouldRetryOnDifferentHost(
                            ex, MAX_RETRIES_PER_HOST + 1, handlerLegacy.getStrategy()))
                    .describedAs(
                            "If the max retries per host has been exceeded, we should always retry on a"
                                    + " different host - but we didn't for exception %s",
                            ex)
                    .isTrue();
        }
    }

    @Test
    public void nonImplicatingExceptionsShouldNeverBlacklist() {
        NOT_IMPLICATING_NODES_EXCEPTIONS.forEach(ex -> {
            assertThat(handlerConservative.shouldDenylist(ex, MAX_RETRIES_PER_HOST + 1))
                    .isFalse();
            assertThat(handlerLegacy.shouldDenylist(ex, MAX_RETRIES_PER_HOST + 1))
                    .isFalse();
        });
    }

    @Test
    public void nonImplicatingExceptionWithConnectionAsCauseShouldNeverBlacklist() {
        Exception ex = new InsufficientConsistencyException("insufficient consistency", new SocketTimeoutException());
        assertThat(handlerConservative.shouldDenylist(ex, MAX_RETRIES_PER_HOST + 1))
                .isFalse();
        assertThat(handlerLegacy.shouldDenylist(ex, MAX_RETRIES_PER_HOST + 1)).isFalse();
    }

    private boolean mutableMode() {
        return currentMode;
    }

    private void flipMode() {
        currentMode = !currentMode;
    }
}

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

package com.palantir.atlasdb.keyvalue.cassandra;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.net.SocketTimeoutException;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.transport.TTransportException;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;

public class CassandraRequestExceptionHandlerTest {
    private static final String MESSAGE = "a exception";
    private static final Exception CAUSE = new Exception();
    private static final CassandraKeyValueServiceConfig config = mock(CassandraKeyValueServiceConfig.class);
    private static final int MAX_RETRIES_PER_HOST = 3;
    private static final int MAX_RETRIES_TOTAL = 6;

    private static Set<Exception> connectionExceptions = Sets.newHashSet(new SocketTimeoutException(MESSAGE),
            new CassandraClientFactory.ClientCreationFailedException(MESSAGE, CAUSE));
    private static Set<Exception> transientExceptions = Sets.newHashSet(new TTransportException());
    private static Set<Exception> indicativeOfCassandraLoadException = Sets.newHashSet(new NoSuchElementException(),
            new TimedOutException(), new UnavailableException(), new InsufficientConsistencyException(MESSAGE));
    private static Set<Exception> fastFailoverExceptions = Sets.newHashSet(new InvalidRequestException());

    private boolean currentMode = true;
    private CassandraRequestExceptionHandler handlerDefault;
    private CassandraRequestExceptionHandler handlerConservative;

    @Before
    public void setup() {
        handlerDefault = new CassandraRequestExceptionHandler(
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
        Set<Exception> allExceptions = new HashSet<>();
        allExceptions = Sets.union(allExceptions, connectionExceptions);
        allExceptions = Sets.union(allExceptions, transientExceptions);
        allExceptions = Sets.union(allExceptions, indicativeOfCassandraLoadException);
        allExceptions = Sets.union(allExceptions, fastFailoverExceptions);

        for (Exception ex : allExceptions) {
            assertTrue(String.format("Exception %s should be retryable", ex), handlerDefault.isRetryable(ex));
        }
        assertFalse("RuntimeException is not retryable", handlerDefault.isRetryable(new RuntimeException()));
    }

    @Test
    public void connectionExceptionsWithSufficientAttemptsShouldBlacklistDefault() {
        for (Exception ex : connectionExceptions) {
            assertFalse("MAX_RETRIES_PER_HOST - 1 attempts should not blacklist",
                    handlerDefault.shouldBlacklist(ex, MAX_RETRIES_PER_HOST - 1));
        }

        for (Exception ex : connectionExceptions) {
            assertTrue(String.format("MAX_RETRIES_PER_HOST attempts with exception %s should blacklist", ex),
                    handlerDefault.shouldBlacklist(ex, MAX_RETRIES_PER_HOST));
        }

        Exception ffException = Iterables.get(fastFailoverExceptions, 0);
        assertFalse(String.format("Exception %s should not blacklist", ffException),
                handlerDefault.shouldBlacklist(ffException, MAX_RETRIES_PER_HOST));
    }

    @Test
    public void connectionExceptionsShouldBackoffDefault() {
        for (Exception ex : connectionExceptions) {
            assertTrue(String.format("Exception %s should backoff", ex),
                    handlerDefault.shouldBackoff(ex, handlerDefault.getStrategy()));
        }
    }

    @Test
    public void cassandraLoadExceptionsShouldBackoffDefault() {
        for (Exception ex : indicativeOfCassandraLoadException) {
            assertTrue(String.format("Exception %s should backoff", ex),
                    handlerDefault.shouldBackoff(ex, handlerDefault.getStrategy()));
        }
    }

    @Test
    public void transientExceptionsShouldNotBackoffDefault() {
        for (Exception ex : transientExceptions) {
            assertFalse(handlerDefault.shouldBackoff(ex, handlerDefault.getStrategy()));
        }
    }

    @Test
    public void fastFailoverExceptionsShouldNotBackoffDefault() {
        for (Exception ex : fastFailoverExceptions) {
            assertFalse(String.format("Exception %s should not backoff", ex),
                    handlerDefault.shouldBackoff(ex, handlerDefault.getStrategy()));
        }
    }

    @Test
    public void connectionExceptionRetriesOnDifferentHostAfterSufficientRetriesDefault() {
        for (Exception ex : connectionExceptions) {
            assertFalse(String.format("Exception %s should not retry on different host", ex),
                    handlerDefault.shouldRetryOnDifferentHost(ex, MAX_RETRIES_PER_HOST - 1,
                            handlerDefault.getStrategy()));
        }

        for (Exception ex : connectionExceptions) {
            assertTrue(String.format("Exception %s should retry on different host", ex),
                    handlerDefault.shouldRetryOnDifferentHost(ex, MAX_RETRIES_PER_HOST, handlerDefault.getStrategy()));
        }
    }

    @Test
    public void cassandraLoadExceptionRetriesOnSameHostDefault() {
        for (Exception ex : indicativeOfCassandraLoadException) {
            assertFalse(handlerDefault.shouldRetryOnDifferentHost(ex, 0, handlerDefault.getStrategy()));
        }
    }

    @Test
    public void cassandraLoadExceptionRetriesOnDifferentHostAfterSufficientRetriesDefault() {
        for (Exception ex : indicativeOfCassandraLoadException) {
            assertTrue(String.format("Exception %s should retry on different host", ex),
                    handlerDefault.shouldRetryOnDifferentHost(ex, MAX_RETRIES_PER_HOST, handlerDefault.getStrategy()));
        }
    }

    @Test
    public void fastFailoverExceptionAlwaysRetriesOnDifferentHostDefault() {
        for (Exception ex : fastFailoverExceptions) {
            assertTrue(String.format("Fast failover exception %s should always retry on different host", ex),
                    handlerDefault.shouldRetryOnDifferentHost(ex, 0, handlerDefault.getStrategy()));
        }
    }

    @Test
    public void retryableExceptionsAreRetryableConservative() {
        Set<Exception> allExceptions = new HashSet<>();
        allExceptions = Sets.union(allExceptions, connectionExceptions);
        allExceptions = Sets.union(allExceptions, transientExceptions);
        allExceptions = Sets.union(allExceptions, indicativeOfCassandraLoadException);
        allExceptions = Sets.union(allExceptions, fastFailoverExceptions);

        for (Exception ex : allExceptions) {
            assertTrue(String.format("Exception %s should be retryable", ex), handlerConservative.isRetryable(ex));
        }
        assertFalse("RuntimeException is not retryable", handlerConservative.isRetryable(new RuntimeException()));
    }

    @Test
    public void connectionExceptionsWithSufficientAttemptsShouldBlacklistConservative() {
        for (Exception ex : connectionExceptions) {
            assertFalse("MAX_RETRIES_PER_HOST - 1 attempts should not blacklist",
                    handlerConservative.shouldBlacklist(ex, MAX_RETRIES_PER_HOST - 1));
        }

        for (Exception ex : connectionExceptions) {
            assertTrue(String.format("MAX_RETRIES_PER_HOST attempts with exception %s should blacklist", ex),
                    handlerConservative.shouldBlacklist(ex, MAX_RETRIES_PER_HOST));
        }

        Exception ffException = Iterables.get(fastFailoverExceptions, 0);
        assertFalse(String.format("Exception %s should not blacklist", ffException),
                handlerConservative.shouldBlacklist(ffException, MAX_RETRIES_PER_HOST));
    }

    @Test
    public void connectionExceptionsShouldBackoffConservative() {
        for (Exception ex : connectionExceptions) {
            assertTrue(String.format("Exception %s should backoff", ex),
                    handlerConservative.shouldBackoff(ex, handlerConservative.getStrategy()));
        }
    }

    @Test
    public void cassandraLoadExceptionsShouldBackoffConservative() {
        for (Exception ex : indicativeOfCassandraLoadException) {
            assertTrue(String.format("Exception %s should backoff", ex),
                    handlerConservative.shouldBackoff(ex, handlerConservative.getStrategy()));
        }
    }

    @Test
    public void transientExceptionsShouldBackoffConservative() {
        for (Exception ex : transientExceptions) {
            assertTrue(handlerConservative.shouldBackoff(ex, handlerConservative.getStrategy()));
        }
    }

    @Test
    public void fastFailoverExceptionsShouldNotBackoffConservative() {
        for (Exception ex : fastFailoverExceptions) {
            assertFalse(String.format("Exception %s should not backoff", ex),
                    handlerConservative.shouldBackoff(ex, handlerConservative.getStrategy()));
        }
    }

    @Test
    public void connectionExceptionRetriesOnDifferentHostAfterSufficientRetriesConservative() {
        for (Exception ex : connectionExceptions) {
            assertFalse(String.format("Exception %s should not retry on different host", ex),
                    handlerConservative.shouldRetryOnDifferentHost(ex, MAX_RETRIES_PER_HOST - 1,
                            handlerConservative.getStrategy()));
        }

        for (Exception ex : connectionExceptions) {
            assertTrue(String.format("Exception %s should retry on different host", ex),
                    handlerConservative.shouldRetryOnDifferentHost(ex, MAX_RETRIES_PER_HOST,
                            handlerConservative.getStrategy()));
        }
    }

    @Test
    public void cassandraLoadExceptionRetriesOnDifferentHostWithoutRetryingConservative() {
        for (Exception ex : indicativeOfCassandraLoadException) {
            assertTrue(handlerConservative.shouldRetryOnDifferentHost(ex, 0, handlerConservative.getStrategy()));
        }
    }

    @Test
    public void cassandraLoadExceptionRetriesOnDifferentHostAfterSufficientRetriesConservative() {
        for (Exception ex : indicativeOfCassandraLoadException) {
            assertTrue(String.format("Exception %s should retry on different host", ex),
                    handlerConservative.shouldRetryOnDifferentHost(ex, MAX_RETRIES_PER_HOST,
                            handlerConservative.getStrategy()));
        }
    }

    @Test
    public void fastFailoverExceptionAlwaysRetriesOnDifferentHostConservative() {
        for (Exception ex : fastFailoverExceptions) {
            assertTrue(String.format("Fast failover exception %s should always retry on different host", ex),
                    handlerConservative.shouldRetryOnDifferentHost(ex, 0, handlerConservative.getStrategy()));
        }
    }

    @Test
    public void changingHandlerModeHasNoEffectWithoutGetStrategy() {
        Exception ex = Iterables.get(transientExceptions, 0);
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

    private boolean mutableMode() {
        return currentMode;
    }

    private void flipMode() {
        currentMode = !currentMode;
    }
}

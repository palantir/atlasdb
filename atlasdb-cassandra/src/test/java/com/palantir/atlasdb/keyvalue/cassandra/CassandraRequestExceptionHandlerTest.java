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

    private Set<Exception> connectionExceptions = Sets.newHashSet(new SocketTimeoutException(MESSAGE),
            new CassandraClientFactory.ClientCreationFailedException(MESSAGE, CAUSE));
    private Set<Exception> transientExceptions = Sets.newHashSet(new TTransportException(),
            new InsufficientConsistencyException(MESSAGE));
    private Set<Exception> indicativeOfCassandraLoadException = Sets.newHashSet(new NoSuchElementException(),
            new TimedOutException(), new UnavailableException());
    private Set<Exception> fastFailoverExceptions = Sets.newHashSet(new InvalidRequestException());

    private DefaultRequestExceptionHandler defaultHandler = new DefaultRequestExceptionHandler(
            () -> MAX_RETRIES_PER_HOST,
            () -> MAX_RETRIES_TOTAL,
            new Blacklist(config));

    private ConservativeRequestExceptionHandler conservativeHandler = new ConservativeRequestExceptionHandler(
            () -> MAX_RETRIES_PER_HOST,
            () -> MAX_RETRIES_TOTAL,
            new Blacklist(config));

    @Test
    public void testShouldRetryTest() {
        Set<Exception> allExceptions = new HashSet<>();
        allExceptions = Sets.union(allExceptions, connectionExceptions);
        allExceptions = Sets.union(allExceptions, transientExceptions);
        allExceptions = Sets.union(allExceptions, indicativeOfCassandraLoadException);
        allExceptions = Sets.union(allExceptions, fastFailoverExceptions);

        for (Exception ex : allExceptions) {
            assertTrue(String.format("Exception %s should be retryable", ex), defaultHandler.isRetryable(ex));
            assertTrue(String.format("Exception %s should be retryable", ex), conservativeHandler.isRetryable(ex));
        }

        assertFalse("RuntimeException is not retryable", defaultHandler.isRetryable(new RuntimeException()));
        assertFalse("RuntimeException is not retryable", conservativeHandler.isRetryable(new RuntimeException()));
    }

    @Test
    public void shouldBlacklistTest() {
        for (Exception ex : connectionExceptions) {
            assertFalse("MAX_RETRIES_PER_HOST - 1 attempts should not blacklist",
                    defaultHandler.shouldBlacklist(ex, MAX_RETRIES_PER_HOST - 1));
            assertFalse("MAX_RETRIES_PER_HOST - 1 attempts should not blacklist",
                    conservativeHandler.shouldBlacklist(ex, MAX_RETRIES_PER_HOST - 1));
        }

        for (Exception ex : connectionExceptions) {
            assertTrue(String.format("MAX_RETRIES_PER_HOST attempts with exception %s should blacklist", ex),
                    defaultHandler.shouldBlacklist(ex, MAX_RETRIES_PER_HOST));
            assertTrue(String.format("MAX_RETRIES_PER_HOST attempts with exception %s should blacklist", ex),
                    conservativeHandler.shouldBlacklist(ex, MAX_RETRIES_PER_HOST));
        }

        Exception ffException = Iterables.get(fastFailoverExceptions, 0);
        assertFalse(String.format("Exception %s should not blacklist", ffException),
                defaultHandler.shouldBlacklist(ffException, MAX_RETRIES_PER_HOST));
        assertFalse(String.format("Exception %s should not blacklist", ffException),
                conservativeHandler.shouldBlacklist(ffException, MAX_RETRIES_PER_HOST));
    }

    @Test
    public void connectionExceptionsShouldBackoffTest() {
        for (Exception ex : connectionExceptions) {
            assertTrue(String.format("Exception %s should backoff", ex),
                    defaultHandler.shouldBackoff(ex));
            assertTrue(String.format("Exception %s should backoff", ex),
                    conservativeHandler.shouldBackoff(ex));
        }
    }

    @Test
    public void cassandraLoadExceptionsShouldBackoffTest() {
        for (Exception ex : indicativeOfCassandraLoadException) {
            assertTrue(String.format("Exception %s should backoff", ex),
                    defaultHandler.shouldBackoff(ex));
            assertTrue(String.format("Exception %s should backoff", ex),
                    conservativeHandler.shouldBackoff(ex));
        }
    }

    @Test
    public void transientExceptionsShouldBackoffForConservativeOnly() {
        for (Exception ex : transientExceptions) {
            assertFalse(String.format("Exception %s should not backoff", ex),
                    defaultHandler.shouldBackoff(ex));
            assertTrue(String.format("Exception %s should backoff", ex),
                    conservativeHandler.shouldBackoff(ex));
        }
    }

    @Test
    public void fastFailoverExceptionsShouldNotBackoff() {
        for (Exception ex : fastFailoverExceptions) {
            assertFalse(String.format("Exception %s should not backoff", ex),
                    defaultHandler.shouldBackoff(ex));
            assertFalse(String.format("Exception %s should not backoff", ex),
                    conservativeHandler.shouldBackoff(ex));
        }
    }

    @Test
    public void connectionExceptionRetriesOnDifferentHostAfterSufficientRetries() {
        for (Exception ex : connectionExceptions) {
            assertFalse(String.format("Exception %s should not retry on different host", ex),
                    defaultHandler.shouldRetryOnDifferentHost(ex, MAX_RETRIES_PER_HOST - 1));
            assertFalse(String.format("Exception %s should not retry on different host", ex),
                    conservativeHandler.shouldRetryOnDifferentHost(ex, MAX_RETRIES_PER_HOST - 1));
        }

        for (Exception ex : connectionExceptions) {
            assertTrue(String.format("Exception %s should retry on different host", ex),
                    defaultHandler.shouldRetryOnDifferentHost(ex, MAX_RETRIES_PER_HOST));
            assertTrue(String.format("Exception %s should retry on different host", ex),
                    conservativeHandler.shouldRetryOnDifferentHost(ex, MAX_RETRIES_PER_HOST));
        }
    }

    @Test
    public void cassandraLoadExceptionRetriesOnDifferentHostAfterSufficientRetriesForDefaultHandler() {
        for (Exception ex : indicativeOfCassandraLoadException) {
            assertFalse(String.format("Exception %s should not retry on different host", ex),
                    defaultHandler.shouldRetryOnDifferentHost(ex, MAX_RETRIES_PER_HOST - 1));
        }

        for (Exception ex : indicativeOfCassandraLoadException) {
            assertTrue(String.format("Exception %s should retry on different host", ex),
                    defaultHandler.shouldRetryOnDifferentHost(ex, MAX_RETRIES_PER_HOST));
        }
    }

    @Test
    public void cassandraLoadExceptionAlwaysRetriesOnDifferentHostForConservativeHandler() {
        for (Exception ex : indicativeOfCassandraLoadException) {
            assertTrue(String.format("Exception %s should retry on different host", ex),
                    conservativeHandler.shouldRetryOnDifferentHost(ex, MAX_RETRIES_PER_HOST - 1));
        }

        for (Exception ex : indicativeOfCassandraLoadException) {
            assertTrue(String.format("Exception %s should retry on different host", ex),
                    defaultHandler.shouldRetryOnDifferentHost(ex, MAX_RETRIES_PER_HOST));
        }
    }

    @Test
    public void fastFailoverExceptionAlwaysRetriesOnDifferentHost() {
        for (Exception ex : fastFailoverExceptions) {
            assertTrue(String.format("Fast failover exception %s should always retry on different host", ex),
                    defaultHandler.shouldRetryOnDifferentHost(ex, 0));
            assertTrue(String.format("Fast failover exception %s should always retry on different host", ex),
                    conservativeHandler.shouldRetryOnDifferentHost(ex, 0));        }
    }
}

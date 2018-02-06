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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.transport.TTransportException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;

@RunWith(Parameterized.class)
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

    private boolean currentMode;
    private CassandraRequestExceptionHandler handler;

    @Parameterized.Parameters
    public static Iterable<? extends Object> data() {
        return Arrays.asList(false, true);
    }

    @Parameterized.Parameter
    public boolean useConservative;

    @Before
    public void setup() {
        currentMode = useConservative;
        handler  = new CassandraRequestExceptionHandler(
                () -> MAX_RETRIES_PER_HOST,
                () -> MAX_RETRIES_TOTAL,
                this::mutableMode,
                new Blacklist(config));
    }

    @Test
    public void retryableExceptionsAreRetryable() {
        Set<Exception> allExceptions = new HashSet<>();
        allExceptions = Sets.union(allExceptions, connectionExceptions);
        allExceptions = Sets.union(allExceptions, transientExceptions);
        allExceptions = Sets.union(allExceptions, indicativeOfCassandraLoadException);
        allExceptions = Sets.union(allExceptions, fastFailoverExceptions);

        for (Exception ex : allExceptions) {
            assertTrue(String.format("Exception %s should be retryable", ex), handler.isRetryable(ex));
        }
        assertFalse("RuntimeException is not retryable", handler.isRetryable(new RuntimeException()));
    }

    @Test
    public void connectionExceptionsWithSufficientAttemptsShouldBlacklist() {
        for (Exception ex : connectionExceptions) {
            assertFalse("MAX_RETRIES_PER_HOST - 1 attempts should not blacklist",
                    handler.shouldBlacklist(ex, MAX_RETRIES_PER_HOST - 1));
        }

        for (Exception ex : connectionExceptions) {
            assertTrue(String.format("MAX_RETRIES_PER_HOST attempts with exception %s should blacklist", ex),
                    handler.shouldBlacklist(ex, MAX_RETRIES_PER_HOST));
        }

        Exception ffException = Iterables.get(fastFailoverExceptions, 0);
        assertFalse(String.format("Exception %s should not blacklist", ffException),
                handler.shouldBlacklist(ffException, MAX_RETRIES_PER_HOST));
    }

    @Test
    public void connectionExceptionsShouldBackoff() {
        for (Exception ex : connectionExceptions) {
            assertTrue(String.format("Exception %s should backoff", ex), handler.shouldBackoff(ex));
        }
    }

    @Test
    public void cassandraLoadExceptionsShouldBackoff() {
        for (Exception ex : indicativeOfCassandraLoadException) {
            assertTrue(String.format("Exception %s should backoff", ex), handler.shouldBackoff(ex));
        }
    }

    @Test
    public void transientExceptionsShouldBackoffForConservativeOnly() {
        for (Exception ex : transientExceptions) {
            assertEquals(useConservative, handler.shouldBackoff(ex));
        }
    }

    @Test
    public void fastFailoverExceptionsShouldNotBackoff() {
        for (Exception ex : fastFailoverExceptions) {
            assertFalse(String.format("Exception %s should not backoff", ex), handler.shouldBackoff(ex));
        }
    }

    @Test
    public void connectionExceptionRetriesOnDifferentHostAfterSufficientRetries() {
        for (Exception ex : connectionExceptions) {
            assertFalse(String.format("Exception %s should not retry on different host", ex),
                    handler.shouldRetryOnDifferentHost(ex, MAX_RETRIES_PER_HOST - 1));
        }

        for (Exception ex : connectionExceptions) {
            assertTrue(String.format("Exception %s should retry on different host", ex),
                    handler.shouldRetryOnDifferentHost(ex, MAX_RETRIES_PER_HOST));
        }
    }

    @Test
    public void cassandraLoadExceptionRetriesOnDifferentHostWithoutRetryingForConservativeHandlerOnly() {
        for (Exception ex : indicativeOfCassandraLoadException) {
            assertEquals(useConservative, handler.shouldRetryOnDifferentHost(ex, 0));
        }
    }

    @Test
    public void cassandraLoadExceptionRetriesOnDifferentHostAfterSufficientRetries() {
        for (Exception ex : indicativeOfCassandraLoadException) {
            assertTrue(String.format("Exception %s should retry on different host", ex),
                    handler.shouldRetryOnDifferentHost(ex, MAX_RETRIES_PER_HOST));
        }
    }

    @Test
    public void fastFailoverExceptionAlwaysRetriesOnDifferentHost() {
        for (Exception ex : fastFailoverExceptions) {
            assertTrue(String.format("Fast failover exception %s should always retry on different host", ex),
                    handler.shouldRetryOnDifferentHost(ex, 0));
        }
    }

    @Test
    public void changingHandlerModeHasNoEffectUntilUpdateStrategy() {
        Exception ex = Iterables.get(transientExceptions, 0);
        assertEquals(useConservative, handler.shouldBackoff(ex));

        flipMode();
        assertEquals(useConservative, handler.shouldBackoff(ex));

        handler.updateStrategy();
        assertEquals(!useConservative, handler.shouldBackoff(ex));
    }

    private boolean mutableMode() {
        return currentMode;
    }

    private void flipMode() {
        currentMode = !currentMode;
    }
}

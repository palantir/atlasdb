/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.timelock.atomix;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;

import org.junit.Before;
import org.junit.Test;

import com.github.rholder.retry.RetryException;
import com.google.common.util.concurrent.UncheckedExecutionException;

import io.atomix.Atomix;
import io.atomix.catalyst.concurrent.Futures;
import io.atomix.copycat.session.ClosedSessionException;
import io.atomix.variables.DistributedLong;

public class AtomixRetryerTest {
    private static final String LONG_KEY = "long";

    private Atomix atomix;

    @Before
    public void setUp() {
        atomix = mock(Atomix.class);
    }

    @Test
    public void retriesOperationsWithClosedSessionException() {
        when(atomix.getLong(LONG_KEY)).thenReturn(Futures.exceptionalFuture(new ClosedSessionException()));
        try {
            AtomixRetryer.getWithRetry(() -> atomix.getLong(LONG_KEY));
            fail();
        } catch (RuntimeException e) {
            assertThat(e.getCause()).isInstanceOf(RetryException.class);
        }
        verify(atomix, times(AtomixRetryer.RETRY_ATTEMPTS)).getLong(eq(LONG_KEY));
    }

    @Test
    public void doesNotRetrySuccessfulOperations() {
        CompletableFuture<DistributedLong> happyFuture = new CompletableFuture<>();
        DistributedLong distributedLong = mock(DistributedLong.class);
        happyFuture.complete(distributedLong);
        when(atomix.getLong(LONG_KEY)).thenReturn(happyFuture);

        DistributedLong returnedLong = AtomixRetryer.getWithRetry(() -> atomix.getLong(LONG_KEY));

        assertThat(returnedLong).isEqualTo(distributedLong);
        verify(atomix, times(1)).getLong(eq(LONG_KEY));
    }

    @Test
    public void rethrowsOtherExceptions() {
        when(atomix.getLong(LONG_KEY)).thenReturn(Futures.exceptionalFuture(new IllegalArgumentException("foo")));
        try {
            AtomixRetryer.getWithRetry(() -> atomix.getLong(LONG_KEY));
            fail();
        } catch (UncheckedExecutionException e) {
            assertThat(e.getCause()).isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("foo");
        }
        verify(atomix, times(1)).getLong(eq(LONG_KEY));
    }
}

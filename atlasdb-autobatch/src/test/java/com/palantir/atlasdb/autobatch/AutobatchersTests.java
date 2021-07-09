/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.autobatch;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;

public class AutobatchersTests {
    @Test
    public void testUnderlyingUncheckedException() {
        DisruptorAutobatcher<Object, Object> autobatcher = Autobatchers.independent(list -> {
                    throw new SafeIllegalStateException("boo");
                })
                .safeLoggablePurpose("testing")
                .build();
        ListenableFuture<Object> response = autobatcher.apply(new Object());
        assertThatThrownBy(response::get)
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(SafeIllegalStateException.class);
    }

    @Test
    public void testTimeoutAndRetryOperations() {
        AtomicLong guard = new AtomicLong(0);
        DisruptorAutobatcher<Object, Object> autobatcher = Autobatchers.independent(list -> {
                    if (guard.compareAndSet(0, 1)) {
                        Uninterruptibles.sleepUninterruptibly(30, TimeUnit.SECONDS);
                    }
                    list.forEach(element -> element.result().set(new Object()));
                })
                .batchFunctionTimeout(Duration.ofSeconds(1))
                .safeLoggablePurpose("testing")
                .build();

        ListenableFuture<Object> response = autobatcher.apply(new Object());

        // Ensure that the second object is not put in the first batch
        Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
        ListenableFuture<Object> secondResponse = autobatcher.apply(new Object());

        // Notice that an exception here implies that we must have timed out prematurely, because nothing else would
        // apply a timeout (without the underlying layer, this call will not throw).
        assertThatThrownBy(response::get)
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(RuntimeException.class)
                .hasRootCauseInstanceOf(TimeoutException.class);

        // Without timeouts, this operation would never succeed!
        assertThatCode(secondResponse::get).doesNotThrowAnyException();
    }
}

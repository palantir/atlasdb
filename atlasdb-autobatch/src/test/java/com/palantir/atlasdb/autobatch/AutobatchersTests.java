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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
    public void testTimeoutOnSlowOrHangingFunction() {
        DisruptorAutobatcher<Object, Object> autobatcher = Autobatchers.independent(list -> {
                    Uninterruptibles.sleepUninterruptibly(30, TimeUnit.SECONDS);
                    list.forEach(element -> element.result().set(new Object()));
                })
                .batchFunctionTimeout(Duration.ofMillis(1))
                .safeLoggablePurpose("testing")
                .build();

        Instant start = Instant.now();
        ListenableFuture<Object> response = autobatcher.apply(new Object());
        assertThatThrownBy(response::get)
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(RuntimeException.class)
                .hasRootCauseInstanceOf(TimeoutException.class);
        assertThat(Duration.between(start, Instant.now())).isLessThan(Duration.ofSeconds(30));
    }
}

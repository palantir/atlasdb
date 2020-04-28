/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.futures;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.Future;

import org.junit.Test;

import com.google.common.util.concurrent.SettableFuture;

public class AtlasFuturesTest {

    @Test
    public void testAtlasFuturesResetsInterrupted() {
        Thread.currentThread().interrupt();
        assertThatThrownBy(() -> AtlasFutures.getUnchecked(SettableFuture.create()))
                .hasRootCauseExactlyInstanceOf(InterruptedException.class);
        assertThat(Thread.interrupted())
                .as("Expected getUnchecked to retain thread interruption state")
                .isTrue();
    }

    @Test
    public void testAtlasFuturesCancelsDelegateOnInterruption() {
        Thread.currentThread().interrupt();
        Future<Object> delegate = SettableFuture.create();
        assertThatThrownBy(() -> AtlasFutures.getUnchecked(delegate))
                .hasRootCauseExactlyInstanceOf(InterruptedException.class);
        assertThat(delegate).isCancelled();
    }
}

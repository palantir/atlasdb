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
package com.palantir.async.initializer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.immutables.value.Value;
import org.junit.Before;
import org.junit.Test;

public class LambdaCallbackTest {
    private static final RuntimeException INIT_FAIL = new RuntimeException("Failed during initialization.");
    private static final RuntimeException CLEANUP_FAIL = new RuntimeException("Failed during cleanup.");
    private ModifiableInitsAndCleanups initsAndCleanups;
    private Callback<ModifiableInitsAndCleanups> callback;

    @Before
    public void setup() {
        initsAndCleanups = InitsAndCleanups.createInitialized();
    }

    @Test
    public void singleAttemptCallbackCallsInitializeOnceAndSkipsCleanupOnSuccess() {
        callback = LambdaCallback.singleAttempt(this::markInit, this::markCleanup);

        callback.runWithRetry(initsAndCleanups);
        assertThat(initsAndCleanups.inits()).isEqualTo(1);
        assertThat(initsAndCleanups.cleanups()).isEqualTo(0);
    }

    @Test
    public void singleAttemptCallbackCallsInitializeAndCleanupOnceThenRethrowsOnInitFailure() {
        callback = LambdaCallback.singleAttempt(this::markInitAndFail, this::markCleanup);

        assertThatThrownBy(() -> callback.runWithRetry(initsAndCleanups)).isEqualToComparingFieldByField(INIT_FAIL);
        assertThat(initsAndCleanups.inits()).isEqualTo(1);
        assertThat(initsAndCleanups.cleanups()).isEqualTo(1);
    }

    @Test
    public void singleAttemptCallbackThrowsCleanupExceptionOnCleanupFailure() {
        callback = LambdaCallback.singleAttempt(this::markInitAndFail, this::markCleanupAndFail);

        assertThatThrownBy(() -> callback.runWithRetry(initsAndCleanups)).isEqualToComparingFieldByField(CLEANUP_FAIL);
    }

    @Test
    public void retryingCallbackCallsInitializeOnceAndSkipsCleanupOnSuccess() {
        callback = LambdaCallback.retrying(this::markInit, this::markCleanup);

        callback.runWithRetry(initsAndCleanups);
        assertThat(initsAndCleanups.inits()).isEqualTo(1);
        assertThat(initsAndCleanups.cleanups()).isEqualTo(0);
    }

    @Test
    public void retryingCallbackCallsInitializeAndCleanupUntilSuccess() {
        callback = LambdaCallback.retrying(this::markInitThenFailIfFewerThatTenInits, this::markCleanup);

        callback.runWithRetry(initsAndCleanups);
        assertThat(initsAndCleanups.inits()).isEqualTo(10);
        assertThat(initsAndCleanups.cleanups()).isEqualTo(9);
    }

    @Test
    public void retryingCallbackStopsRetryingWhenCleanupThrows() {
        callback = LambdaCallback.retrying(this::markInitAndFail, this::markCleanupThenFailIfMoreThatTenInits);

        assertThatThrownBy(() -> callback.runWithRetry(initsAndCleanups)).isEqualToComparingFieldByField(CLEANUP_FAIL);
        assertThat(initsAndCleanups.inits()).isEqualTo(11);
        assertThat(initsAndCleanups.cleanups()).isEqualTo(11);
    }

    private void markInit(ModifiableInitsAndCleanups initsAndCleanups) {
        initsAndCleanups.setInits(initsAndCleanups.inits() + 1);
    }

    private void markInitAndFail(ModifiableInitsAndCleanups initsAndCleanups) {
        markInit(initsAndCleanups);
        throw INIT_FAIL;
    }

    private void markInitThenFailIfFewerThatTenInits(ModifiableInitsAndCleanups initsAndCleanups) {
        markInit(initsAndCleanups);
        if (initsAndCleanups.inits() < 10) {
            throw INIT_FAIL;
        }
    }

    private void markCleanup(ModifiableInitsAndCleanups initsAndCleanups, Throwable ignore) {
        initsAndCleanups.setCleanups(initsAndCleanups.cleanups() + 1);
    }

    private void markCleanupAndFail(ModifiableInitsAndCleanups initsAndCleanups, Throwable ignore) {
        markCleanup(initsAndCleanups, ignore);
        throw CLEANUP_FAIL;
    }

    private void markCleanupThenFailIfMoreThatTenInits(ModifiableInitsAndCleanups initsAndCleanups, Throwable ignore) {
        markCleanup(initsAndCleanups, ignore);
        if (initsAndCleanups.inits() > 10) {
            throw CLEANUP_FAIL;
        }
    }

    @Value.Modifiable
    interface InitsAndCleanups {
        int inits();

        int cleanups();

        static ModifiableInitsAndCleanups createInitialized() {
            return ModifiableInitsAndCleanups.create().setInits(0).setCleanups(0);
        }
    }
}

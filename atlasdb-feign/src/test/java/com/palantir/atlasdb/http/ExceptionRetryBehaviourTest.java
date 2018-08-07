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
package com.palantir.atlasdb.http;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.Date;

import org.junit.Test;

import com.palantir.atlasdb.http.errors.AtlasDbRemoteException;
import com.palantir.common.remoting.ServiceNotAvailableException;
import com.palantir.remoting2.errors.RemoteException;
import com.palantir.remoting2.errors.SerializableError;

import feign.RetryableException;

@SuppressWarnings("ThrowableResultOfMethodCallIgnored") // We need to create exceptions for testing.
public class ExceptionRetryBehaviourTest {
    private static final ServiceNotAvailableException SERVICE_NOT_AVAILABLE_EXCEPTION
            = new ServiceNotAvailableException("foo");
    private static final AtlasDbRemoteException REMOTE_BLOCKING_TIMEOUT_EXCEPTION
            = new AtlasDbRemoteException(
                    new RemoteException(SerializableError.of("foo", ""), 503));
    private static final Date DATE = Date.from(Instant.EPOCH);

    @Test
    public void retryOnOtherNodesShouldRetryInfinitelyManyTimes() {
        ExceptionRetryBehaviour behaviour = ExceptionRetryBehaviour.RETRY_ON_OTHER_NODE;
        assertThat(behaviour.shouldRetryInfinitelyManyTimes()).isTrue();
    }

    @Test
    public void retryOnOtherNodesShouldRetryOnOtherNodes() {
        ExceptionRetryBehaviour behaviour = ExceptionRetryBehaviour.RETRY_ON_OTHER_NODE;
        assertThat(behaviour.shouldBackoffAndTryOtherNodes()).isTrue();
    }

    @Test
    public void retryIndefinitelyOnSameNodeShouldRetryInfinitelyManyTimes() {
        ExceptionRetryBehaviour behaviour = ExceptionRetryBehaviour.RETRY_INDEFINITELY_ON_SAME_NODE;
        assertThat(behaviour.shouldRetryInfinitelyManyTimes()).isTrue();
    }

    @Test
    public void retryIndefinitelyOnSameNodeShouldRetryOnSameNode() {
        ExceptionRetryBehaviour behaviour = ExceptionRetryBehaviour.RETRY_INDEFINITELY_ON_SAME_NODE;
        assertThat(behaviour.shouldBackoffAndTryOtherNodes()).isFalse();
    }

    @Test
    public void retryOnSameNodeShouldRetryFinitelyManyTimes() {
        ExceptionRetryBehaviour behaviour = ExceptionRetryBehaviour.RETRY_ON_SAME_NODE;
        assertThat(behaviour.shouldRetryInfinitelyManyTimes()).isFalse();
    }

    @Test
    public void retryOnSameNodeShouldRetryOnSameNode() {
        ExceptionRetryBehaviour behaviour = ExceptionRetryBehaviour.RETRY_ON_SAME_NODE;
        assertThat(behaviour.shouldBackoffAndTryOtherNodes()).isFalse();
    }

    @Test
    public void shouldRetryIndefinitelyOnSameNodeOnBlockingTimeoutExceptionWithoutRetryAfter() {
        RetryableException exception = createRetryableExceptionWithGenericMessage(
                REMOTE_BLOCKING_TIMEOUT_EXCEPTION, null);
        assertThat(ExceptionRetryBehaviour.getRetryBehaviourForException(exception))
                .isEqualTo(ExceptionRetryBehaviour.RETRY_INDEFINITELY_ON_SAME_NODE);
    }

    @Test
    public void shouldRetryIndefinitelyOnSameNodeOnBlockingTimeoutExceptionWithRetryAfter() {
        RetryableException exception = createRetryableExceptionWithGenericMessage(
                REMOTE_BLOCKING_TIMEOUT_EXCEPTION, DATE);
        assertThat(ExceptionRetryBehaviour.getRetryBehaviourForException(exception))
                .isEqualTo(ExceptionRetryBehaviour.RETRY_INDEFINITELY_ON_SAME_NODE);
    }

    @Test
    public void shouldRetryOnSameNodeOnUnknownCauseRetryableExceptionWithoutRetryAfter() {
        RetryableException exception = createRetryableExceptionWithGenericMessage(
                null, null);
        assertThat(ExceptionRetryBehaviour.getRetryBehaviourForException(exception))
                .isEqualTo(ExceptionRetryBehaviour.RETRY_ON_SAME_NODE);
    }

    @Test
    public void shouldRetryOnOtherNodesOnUnknownCauseRetryableExceptionWithRetryAfter() {
        RetryableException exception = createRetryableExceptionWithGenericMessage(
                null, DATE);
        assertThat(ExceptionRetryBehaviour.getRetryBehaviourForException(exception))
                .isEqualTo(ExceptionRetryBehaviour.RETRY_ON_OTHER_NODE);
    }

    @Test
    public void shouldRetryOnSameNodeOnServiceNotAvailableExceptionWithoutRetryAfter() {
        RetryableException exception = createRetryableExceptionWithGenericMessage(
                SERVICE_NOT_AVAILABLE_EXCEPTION, null);
        assertThat(ExceptionRetryBehaviour.getRetryBehaviourForException(exception))
                .isEqualTo(ExceptionRetryBehaviour.RETRY_ON_SAME_NODE);
    }

    @Test
    public void shouldRetryOnOtherNodesOnServiceNotAvailableExceptionWithRetryAfter() {
        RetryableException exception = createRetryableExceptionWithGenericMessage(
                SERVICE_NOT_AVAILABLE_EXCEPTION, DATE);
        assertThat(ExceptionRetryBehaviour.getRetryBehaviourForException(exception))
                .isEqualTo(ExceptionRetryBehaviour.RETRY_ON_OTHER_NODE);
    }

    private static RetryableException createRetryableExceptionWithGenericMessage(Exception cause, Date retryAfter) {
        return new RetryableException("Timeout", cause, retryAfter);
    }
}

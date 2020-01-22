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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.immutables.value.Value;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

public class CallChainTest {
    private static final String TEST_STRING = "abc";

    private final List<CallbackInvocation> invocations = Lists.newArrayList();

    @Test
    @SuppressWarnings("unchecked") // Mocks are guaranteed to be of a suitable type
    public void callChainInvokesCallbacksInOrder() {
        Callback<String> first = mock(Callback.class);
        Callback<String> second = mock(Callback.class);

        Callback<String> chain = new Callback.CallChain<>(ImmutableList.of(first, second));
        chain.init(TEST_STRING);

        InOrder inOrder = Mockito.inOrder(first, second);
        inOrder.verify(first).runWithRetry(TEST_STRING);
        inOrder.verify(second).runWithRetry(TEST_STRING);
        verifyNoMoreInteractions(first, second);
    }

    @Test
    public void callChainRetriesOneCallbackAtATime() {
        ListUpdatingCallback first = new ListUpdatingCallback(succeedingOnAttemptNumber(2), () -> false);
        ListUpdatingCallback second = new ListUpdatingCallback(succeedingOnAttemptNumber(3), () -> false);

        Callback<String> chain = new Callback.CallChain<>(ImmutableList.of(first, second));
        chain.init(TEST_STRING);

        assertThat(invocations).containsExactly(
                ImmutableCallbackInvocation.of(first, TEST_STRING, InvocationType.INIT),
                ImmutableCallbackInvocation.of(first, TEST_STRING, InvocationType.CLEANUP),
                ImmutableCallbackInvocation.of(first, TEST_STRING, InvocationType.INIT),
                ImmutableCallbackInvocation.of(second, TEST_STRING, InvocationType.INIT),
                ImmutableCallbackInvocation.of(second, TEST_STRING, InvocationType.CLEANUP),
                ImmutableCallbackInvocation.of(second, TEST_STRING, InvocationType.INIT),
                ImmutableCallbackInvocation.of(second, TEST_STRING, InvocationType.CLEANUP),
                ImmutableCallbackInvocation.of(second, TEST_STRING, InvocationType.INIT));
    }

    @Test
    public void callChainPropagatesErrorsInCleanup() {
        ListUpdatingCallback first = new ListUpdatingCallback(() -> true, () -> true);

        Callback<String> chain = new Callback.CallChain<>(ImmutableList.of(first));
        assertCleanupFailPropagated(chain);
    }

    @Test
    public void callChainDoesNotRunSecondCallbackIfFirstFails() {
        ListUpdatingCallback first = new ListUpdatingCallback(() -> true, () -> true);
        ListUpdatingCallback second = new ListUpdatingCallback(() -> true, () -> true);

        Callback<String> chain = new Callback.CallChain<>(ImmutableList.of(first, second));
        assertCleanupFailPropagated(chain);

        assertThat(invocations).containsExactly(
                ImmutableCallbackInvocation.of(first, TEST_STRING, InvocationType.INIT),
                ImmutableCallbackInvocation.of(first, TEST_STRING, InvocationType.CLEANUP));
    }

    @Test
    public void callChainDoesNotAttemptToCleanUpFirstCallbackIfSecondCleanupFails() {
        ListUpdatingCallback first = new ListUpdatingCallback(() -> false, () -> false);
        ListUpdatingCallback second = new ListUpdatingCallback(() -> true, () -> true);

        Callback<String> chain = new Callback.CallChain<>(ImmutableList.of(first, second));
        assertCleanupFailPropagated(chain);

        assertThat(invocations).containsExactly(
                ImmutableCallbackInvocation.of(first, TEST_STRING, InvocationType.INIT),
                ImmutableCallbackInvocation.of(second, TEST_STRING, InvocationType.INIT),
                ImmutableCallbackInvocation.of(second, TEST_STRING, InvocationType.CLEANUP));
    }

    private void assertCleanupFailPropagated(Callback<String> chain) {
        assertThatThrownBy(() -> chain.init(TEST_STRING)).isInstanceOf(RuntimeException.class)
                .hasMessageContaining("cleanup fail");
    }

    private static Supplier<Boolean> succeedingOnAttemptNumber(int number) {
        Preconditions.checkState(number >= 1, "Cannot set-up to first succeed on a non-positive attempt number!");
        Iterator<Boolean> resultIterator =
                Stream.concat(Collections.nCopies(number - 1, true).stream(), Stream.generate(() -> false)).iterator();
        return resultIterator::next;
    }

    private class ListUpdatingCallback extends Callback<String> {
        private final Supplier<Boolean> shouldFailInit;
        private final Supplier<Boolean> shouldFailCleanup;

        private ListUpdatingCallback(Supplier<Boolean> shouldFailInit, Supplier<Boolean> shouldFailCleanup) {
            this.shouldFailInit = shouldFailInit;
            this.shouldFailCleanup = shouldFailCleanup;
        }

        @Override
        public void init(String resource) {
            invocations.add(ImmutableCallbackInvocation.of(this, resource, InvocationType.INIT));
            if (shouldFailInit.get()) {
                throw new RuntimeException("init fail");
            }
        }

        @Override
        public void cleanup(String resource, Throwable initException) {
            invocations.add(ImmutableCallbackInvocation.of(this, resource, InvocationType.CLEANUP));
            if (shouldFailCleanup.get()) {
                throw new RuntimeException("cleanup fail");
            }
        }
    }

    enum InvocationType {
        INIT,
        CLEANUP;
    }

    @Value.Immutable
    interface CallbackInvocation {
        @Value.Parameter
        Callback<String> callback();

        @Value.Parameter
        String argument();

        @Value.Parameter
        InvocationType invocationType();
    }
}

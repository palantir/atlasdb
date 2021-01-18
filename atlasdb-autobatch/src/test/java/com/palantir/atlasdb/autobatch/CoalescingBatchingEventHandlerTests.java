/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.immutables.value.Value;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CoalescingBatchingEventHandlerTests {

    private static final AtomicLong COUNTER = new AtomicLong();

    @Mock
    private CoalescingRequestFunction<Request, Response> function;

    @Test
    public void coalescesIdenticalRequests() throws ExecutionException, InterruptedException {
        Request request1 = ImmutableRequest.of(5);
        Request request2 = ImmutableRequest.of(5);
        Request request3 = ImmutableRequest.of(5);

        assertThat(ImmutableSet.of(request1, request2, request3))
                .as("requests are semantically equivalent")
                .hasSize(1);

        assertThat(request1).isNotSameAs(request2);
        assertThat(request1).isNotSameAs(request3);
        assertThat(request2).isNotSameAs(request3);

        Response response = ImmutableResponse.of(10);

        when(function.apply(ImmutableSet.of(request1))).thenReturn(ImmutableMap.of(request1, response));

        CoalescingBatchingEventHandler<Request, Response> handler =
                new CoalescingBatchingEventHandler<>(function, 5, useCase);
        Future<Response> response1 = addToBatch(handler, request1);
        Future<Response> response2 = addToBatch(handler, request2);
        Future<Response> response3 = addAndEndBatch(handler, request3);

        assertThat(ImmutableList.of(response1.get(), response2.get(), response3.get()))
                .as("same *instance* as response")
                .allSatisfy(r -> assertThat(r).isSameAs(response));
    }

    @Test
    public void differentRequestsGetBatchedAndServedCorrectly() throws ExecutionException, InterruptedException {
        Request request1 = ImmutableRequest.of(5);
        Request request2 = ImmutableRequest.of(10);
        Response responseFor1 = ImmutableResponse.of(-5);
        Response responseFor2 = ImmutableResponse.of(-10);

        when(function.apply(ImmutableSet.of(request1, request2)))
                .thenReturn(ImmutableMap.of(
                        request1, responseFor1,
                        request2, responseFor2));

        CoalescingBatchingEventHandler<Request, Response> handler =
                new CoalescingBatchingEventHandler<>(function, 5, useCase);

        Future<Response> response1Future = addToBatch(handler, request1);
        Future<Response> response2Future = addAndEndBatch(handler, request2);

        assertThat(response1Future.get()).isEqualTo(responseFor1);
        assertThat(response2Future.get()).isEqualTo(responseFor2);
    }

    @Test
    public void throwsInFutureIfConstraintIsViolated() throws ExecutionException, InterruptedException {
        Request request1 = ImmutableRequest.of(5);
        Request request2 = ImmutableRequest.of(10);
        Response responseFor1 = ImmutableResponse.of(-5);

        when(function.apply(ImmutableSet.of(request1, request2))).thenReturn(ImmutableMap.of(request1, responseFor1));

        CoalescingBatchingEventHandler<Request, Response> handler =
                new CoalescingBatchingEventHandler<>(function, 5, useCase);

        Future<Response> response1Future = addToBatch(handler, request1);
        Future<Response> response2Future = addAndEndBatch(handler, request2);

        assertThat(response1Future.get())
                .as("element in result function gets resolved")
                .isEqualTo(responseFor1);

        assertThatThrownBy(response2Future::get)
                .as("element missing from result map resolves to default value")
                .hasCauseInstanceOf(PostconditionFailedException.class);
    }

    @Test
    public void batchesAreRespected() throws ExecutionException, InterruptedException {
        Request request = ImmutableRequest.of(5);
        Response firstInvocation = ImmutableResponse.of(-5);
        Response secondInvocation = ImmutableResponse.of(-10);

        when(function.apply(ImmutableSet.of(request)))
                .thenReturn(ImmutableMap.of(request, firstInvocation))
                .thenReturn(ImmutableMap.of(request, secondInvocation))
                .thenThrow(new AssertionError("should not reach here"));

        CoalescingBatchingEventHandler<Request, Response> handler =
                new CoalescingBatchingEventHandler<>(function, 5, useCase);
        Future<Response> firstInvocationResponse = addAndEndBatch(handler, request);
        Future<Response> secondInvocationResponse = addAndEndBatch(handler, request);

        assertThat(firstInvocationResponse.get())
                .as("first invocation of the same request gets first response")
                .isEqualTo(firstInvocation);

        assertThat(secondInvocationResponse.get())
                .as("second invocation of the same request gets second response")
                .isEqualTo(secondInvocation);
    }

    @Test
    public void exceptionsFailUncompletedElementsInBatch() {
        RuntimeException exception = new RuntimeException("something went wrong");

        when(function.apply(anySet())).thenThrow(exception);

        CoalescingBatchingEventHandler<Request, Response> handler =
                new CoalescingBatchingEventHandler<>(function, 5, useCase);
        Future<Response> firstInvocationResponse = addToBatch(handler, ImmutableRequest.of(5));
        Future<Response> secondInvocationResponse = addAndEndBatch(handler, ImmutableRequest.of(10));

        assertThatExceptionOfType(ExecutionException.class)
                .isThrownBy(firstInvocationResponse::get)
                .withCause(exception);

        assertThatExceptionOfType(ExecutionException.class)
                .isThrownBy(secondInvocationResponse::get)
                .withCause(exception);
    }

    private static Future<Response> addToBatch(
            CoalescingBatchingEventHandler<Request, Response> handler, Request request) {
        return addElementToBatch(handler, request, false);
    }

    private static Future<Response> addAndEndBatch(
            CoalescingBatchingEventHandler<Request, Response> handler, Request request) {
        return addElementToBatch(handler, request, true);
    }

    private static Future<Response> addElementToBatch(
            CoalescingBatchingEventHandler<Request, Response> handler, Request request, boolean endBatch) {
        TestBatchElement element = ImmutableTestBatchElement.of(request);
        handler.onEvent(element, COUNTER.getAndIncrement(), endBatch);
        return element.result();
    }

    @Value.Immutable
    interface Request {
        @Value.Parameter
        int value();
    }

    @Value.Immutable
    interface Response {
        @Value.Parameter
        int value();
    }

    @SuppressWarnings("immutables:subtype")
    @Value.Immutable
    interface TestBatchElement extends BatchElement<Request, Response> {

        @Value.Parameter
        @Override
        Request argument();

        @Value.Derived
        @Override
        default DisruptorAutobatcher.DisruptorFuture<Response> result() {
            return new DisruptorAutobatcher.DisruptorFuture<>("test");
        }
    }
}

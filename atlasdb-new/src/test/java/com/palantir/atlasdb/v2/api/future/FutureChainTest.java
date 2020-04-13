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

package com.palantir.atlasdb.v2.api.future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

@RunWith(MockitoJUnitRunner.class)
public final class FutureChainTest {
    private static final String RESULT = "result";
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final RuntimeException thrown = new RuntimeException();

    @Mock private AsyncFunction<String, String> asyncTransformer;
    @Mock private AsyncFunction<String, String> otherAsyncTransformer;
    @Mock private Function<String, String> transformer;
    @Mock private Predicate<String> predicate;
    @Mock private UnaryOperator<FutureChain<String>> whileLoopBody;
    @Mock private BinaryOperator<String> merger;

    @After
    public void after() {
        executor.shutdown();
    }

    @Test
    public void testMaskedDone_obfuscatesResult() {
        FutureChain<String> chain = FutureChain.start(executor, RESULT);
        assertThat(Futures.getUnchecked(chain.maskedDone())).isNull();
    }

    @Test
    public void testAlterState() {
        String input = "input";
        ListenableFuture<String> chain = FutureChain.start(executor, input)
                .alterState(currentState -> {
                    assertThat(currentState.equals("input"));
                    return RESULT;
                }).done();
        assertThat(Futures.getUnchecked(chain)).isEqualTo(RESULT);
    }

    @Test
    public void testAlterState_failureOnInput() {
        RuntimeException thrown = new RuntimeException();
        ListenableFuture<String> result = FutureChain.<String>start(executor,
                Futures.immediateFailedFuture(thrown))
                .alterState(transformer)
                .done();
        assertThatThrownBy(() -> Futures.getUnchecked(result)).hasCause(thrown);
        verifyNoInteractions(transformer);
    }

    @Test
    public void testAlterState_failuresOnAltering() {
        when(transformer.apply(any())).thenThrow(thrown);
        ListenableFuture<String> result = FutureChain.start(executor,
                "")
                .alterState(transformer)
                .done();
        assertThatThrownBy(() -> Futures.getUnchecked(result)).hasCause(thrown);
    }

    @Test
    public void testWhileTrue() {
        FutureChain<Integer> start = FutureChain.start(executor, 0);
        int limit = 1000;
        assertThat(Futures.getUnchecked(start.whileTrue(i -> i < limit, c -> c.alterState(i -> i + 1)).done()))
                .isEqualTo(limit);
    }

    @Test
    public void testWhileTrue_failuresOnInput() {
        assertFailed(FutureChain.<String>start(executor, Futures.immediateFailedFuture(thrown))
                .whileTrue(predicate, whileLoopBody));
        verifyNoInteractions(predicate);
        verifyNoInteractions(whileLoopBody);
    }

    @Test
    public void testWhileTrue_failuresOnPredicate() {
        when(predicate.test(any())).thenThrow(thrown);
        assertFailed(FutureChain.start(executor, Futures.immediateFuture(""))
                .whileTrue(predicate, whileLoopBody));
        verifyNoInteractions(whileLoopBody);
    }

    @Test
    public void testWhileTrue_failuresOnLoopBody() {
        String input = "input";
        when(whileLoopBody.apply(any())).thenThrow(thrown);
        when(predicate.test(any())).thenReturn(true);
        assertFailed(FutureChain.start(executor, input)
                .whileTrue(predicate, whileLoopBody));
    }

    @Test
    public void testThenWithMerging() {
        String a = "a";
        String b = "b";
        String c = "c";
        assertThat(Futures.getUnchecked(FutureChain.start(executor, a)
                .then(in -> {
                    assertThat(in).isEqualTo(a);
                    return Futures.immediateFuture(b);
                },(outA, outB) -> {
                    assertThat(outA).isEqualTo(a);
                    assertThat(outB).isEqualTo(b);
                    return c;
                }).done())).isEqualTo(c);
    }

    @Test
    public void testThenWithMerging_failuresOnInput() {
        assertFailed(FutureChain.start(executor, Futures.<String>immediateFailedFuture(thrown))
                .then(asyncTransformer, merger));
    }

    @Test
    public void testThenWithMerging_failuresOnOperation() throws Exception {
        String input = "input";
        when(asyncTransformer.apply(input)).thenThrow(thrown);
        assertFailed(FutureChain.start(executor, Futures.immediateFuture(input))
                .then(asyncTransformer, merger));
        verifyNoInteractions(merger);
    }

    @Test
    public void testThenWithMerging_failuresOnMerging() throws Exception {
        String input = "input";
        String output = "output";
        when(asyncTransformer.apply(input)).thenReturn(Futures.immediateFuture(output));
        when(merger.apply(input, output)).thenThrow(thrown);
        assertFailed(FutureChain.start(executor, Futures.immediateFuture(input))
                .then(asyncTransformer, merger));
    }

    @Test
    public void testThenWithStateTransformer() {
        String a = "a";
        String b = "b";
        String c = "c";
        String d = "d";
        assertThat(Futures.getUnchecked(FutureChain.start(executor, a)
                .then(inA -> {
                    assertThat(inA).isEqualTo(a);
                    return Futures.immediateFuture(b);
                }, inA -> {
                    assertThat(inA).isEqualTo(a);
                    return Futures.immediateFuture(c);
                }, (inC, inB) -> {
                    assertThat(inB).isEqualTo(b);
                    assertThat(inC).isEqualTo(c);
                    return d;
                }).done())).isEqualTo(d);
    }

    @Test
    public void testThenWithStateTransformer_failuresOnInput() {
        assertFailed(FutureChain.start(executor, Futures.<String>immediateFailedFuture(thrown))
                .then(asyncTransformer, otherAsyncTransformer, merger));
        verifyNoInteractions(asyncTransformer);
        verifyNoInteractions(otherAsyncTransformer);
        verifyNoInteractions(merger);
    }

    @Test
    public void testThenWithStateTransformer_failuresOnOperation() throws Exception {
        String input = "input";
        when(asyncTransformer.apply(input)).thenThrow(thrown);
        assertFailed(FutureChain.start(executor, Futures.immediateFuture(input))
                .then(asyncTransformer, otherAsyncTransformer, merger));
        verifyNoInteractions(otherAsyncTransformer);
        verifyNoInteractions(merger);
    }

    @Test
    public void testThenWithStateTransformer_failuresOnStateTransformer() throws Exception {
        String input = "input";
        when(asyncTransformer.apply(input)).thenReturn(Futures.immediateFuture("output"));
        when(otherAsyncTransformer.apply(input)).thenThrow(thrown);
        assertFailed(FutureChain.start(executor, input).then(asyncTransformer, otherAsyncTransformer, merger));
        verifyNoInteractions(merger);
    }

    @Test
    public void testThenWithStateTransformer_failuresOnResultMerger() throws Exception {
        String a = "a";
        String b = "b";
        String c = "c";
        when(asyncTransformer.apply(a)).thenReturn(Futures.immediateFuture(b));
        when(otherAsyncTransformer.apply(a)).thenReturn(Futures.immediateFuture(c));
        when(merger.apply(c, b)).thenThrow(thrown);
        assertFailed(FutureChain.start(executor, a)
                .then(asyncTransformer, otherAsyncTransformer, merger));
    }

    @Test
    public void testSoloThen() {
        String input = "input";
        SettableFuture<String> result = SettableFuture.create();
        ListenableFuture<String> future = FutureChain.start(executor, input)
                .then(in -> {
                    assertThat(in).isEqualTo(input);
                    return result;
                }).done();
        assertThat(future).isNotDone();
        result.set("");
        assertThat(Futures.getUnchecked(future)).isEqualTo(input);
    }

    @Test
    public void testSoloThen_failuresOnInput() {
        assertFailed(FutureChain.start(executor, Futures.<String>immediateFailedFuture(thrown))
                .then(asyncTransformer));
        verifyNoInteractions(asyncTransformer);
    }

    @Test
    public void testSoloThen_failuresOnOperation() throws Exception {
        String input = "input";
        when(asyncTransformer.apply(input)).thenThrow(thrown);
        assertFailed(FutureChain.start(executor, Futures.immediateFuture(input))
                .then(asyncTransformer));
    }

    @Test
    public void testDefer() {
        List<Integer> trace = new ArrayList<>();
        SettableFuture<String> deferral = SettableFuture.create();
        Futures.getUnchecked(FutureChain.start(executor, "")
                .alterState(x -> {
                    trace.add(1);
                    return x;
                })
                .defer(x -> {
                    trace.add(2);
                })
                .alterState(x -> {
                    trace.add(3);
                    return x;
                }).defer(x -> {
                    trace.add(4);
                }).done());
        assertThat(trace).containsExactly(1, 3, 4, 2);
    }

    @Test
    public void testDefer_runsIfLaterOperationFails() {
        String a = "a";
        SettableFuture<String> deferral = SettableFuture.create();
        assertFailed(FutureChain.start(executor, a)
                .defer(inA -> {
                    assertThat(inA).isEqualTo(a);
                    deferral.set(inA);
                })
                .alterState(inA -> {
                    assertThat(inA).isEqualTo(a);
                    assertThat(deferral).isNotDone();
                    throw thrown;
                }));
        assertThat(Futures.getUnchecked(deferral)).isEqualTo(a);
    }

    @Test
    public void testDefer_runsAfterLaterOperation() {
        String a = "a";
        String b = "b";
        SettableFuture<String> deferral = SettableFuture.create();
        assertThat(Futures.getUnchecked(FutureChain.start(executor, a)
                .defer(inA -> {
                    assertThat(inA).isEqualTo(a);
                    deferral.set(inA);
                })
                .alterState(inA -> {
                    assertThat(inA).isEqualTo(a);
                    assertThat(deferral).isNotDone();
                    return b;
                }).done())).isEqualTo(b);
        assertThat(Futures.getUnchecked(deferral)).isEqualTo(a);
    }

    private void assertFailed(FutureChain<?> chain) {
        assertThatThrownBy(() -> Futures.getUnchecked(chain.done())).hasCause(thrown);
    }
}

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
package com.palantir.common.proxy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.palantir.common.exception.PalantirRuntimeException;
import com.palantir.exception.PalantirInterruptedException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;

public class InterruptibleProxyTest {

    @Test
    public void testInterrupt() throws InterruptedException, BrokenBarrierException {
        final CyclicBarrier barrier = new CyclicBarrier(2);
        final Thread callingThread = Thread.currentThread();
        final AtomicBoolean gotInterrupted = new AtomicBoolean(false);
        List<String> strings = new ArrayList<String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public String get(int arg0) {
                if (arg0 == 1) {
                    // In order for the get(1) test below to reliably check
                    // interrupt we can't actually finish (otherwise our
                    // completion is racing main thread checking the future and
                    // if we win it gets the value instead of Future.get()
                    // checking interrupt).
                    try {
                        Thread.sleep(Long.MAX_VALUE);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                }
                return super.get(arg0);
            }

            @Override
            public String remove(int arg0) {
                assertThat(callingThread).isNotEqualTo(Thread.currentThread());
                while (true) {
                    try {
                        Thread.sleep(100);
                        callingThread.interrupt();
                    } catch (InterruptedException e) {
                        gotInterrupted.set(true);
                        try {
                            barrier.await();
                        } catch (InterruptedException e1) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e1);
                        } catch (BrokenBarrierException e1) {
                            throw new RuntimeException(e1);
                        }
                        throw new PalantirRuntimeException(e);
                    }
                }
            }
        };
        strings.add("Foo");

        @SuppressWarnings("unchecked")
        List<String> proxy = InterruptibleProxy.newProxyInstance(List.class, strings, CancelDelegate.CANCEL);
        assertThat(proxy.get(0)).isEqualTo("Foo");
        assertThatThrownBy(() -> {
                    Thread.currentThread().interrupt();
                    assertThat(proxy.get(1)).isEqualTo("Foo");
                })
                .describedAs("Should be interrupted")
                .isInstanceOf(PalantirInterruptedException.class);

        assertThatThrownBy(() -> assertThat(proxy.get(1000)).isEqualTo("Foo"))
                .describedAs("Should throw exception")
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> proxy.remove(0))
                .describedAs("Should be interrupted")
                .isInstanceOf(PalantirInterruptedException.class);

        barrier.await();

        assertThat(gotInterrupted.get()).isTrue();
    }
}

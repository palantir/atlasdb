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

package com.palantir.timelock;

import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import io.reactivex.Observable;

/**
 * Utilities for handling RxJava Observables.
 */
public final class Observables {
    private Observables() {
        // utility class
    }

    /**
     * Returns a {@link Supplier} that always returns the most recent value on the given {@link Observable}, blocking if
     * the observable has not emitted a value when {@link Supplier#get} is called.
     *
     * @throws NoSuchElementException if the given observable emits no items
     */
    public static <T> Supplier<T> blockingMostRecent(Observable<T> obs) {
        AtomicReference<T> reference = new AtomicReference<>();
        subscribe(obs, reference::set);
        return () -> {
            if (reference.get() == null) {
                return obs.blockingFirst();
            }
            return reference.get();
        };
    }

    @SuppressWarnings("CheckReturnValue") // We won't need to unsubscribe, for our use case.
    private static <T> void subscribe(Observable<T> observable, io.reactivex.functions.Consumer<? super T> onNext) {
        observable.subscribe(onNext);
    }
}

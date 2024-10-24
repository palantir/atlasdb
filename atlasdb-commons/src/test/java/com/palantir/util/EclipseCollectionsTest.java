/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.util;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class EclipseCollectionsTest {

    /**
     * See JDK bugs with deadlock between JNI loadLibrary and class loading from signed JARs (e.g. eclipse-collections).
     * <ul>
     *     <li><a href="https://bugs.openjdk.org/browse/JDK-8266350">JDK-8266350</a></li>
     *     <li><a href="https://bugs.openjdk.org/browse/JDK-8266310">JDK-8266310</a></li>
     * </ul>
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    // if timeout is exceeded, class load has likely deadlocked
    void canLoadClassesWithoutDeadlock() throws ExecutionException, InterruptedException {
        ListeningExecutorService executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(4));
        try {
            List<ListenableFuture<Boolean>> futures = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                futures.add(executorService.submit(EclipseCollections::loadClasses));
            }
            executorService.shutdown();
            assertThat(Futures.allAsList(futures).get()).hasSize(10).satisfiesOnlyOnce(r -> assertThat(r)
                    .isTrue());
        } finally {
            executorService.shutdownNow();
        }
    }
}

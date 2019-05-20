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

package com.palantir.util;

import static org.assertj.core.api.Java6Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.junit.Test;

import com.palantir.common.concurrent.PTExecutors;

public class MemoizedComposedSupplierTest {
    @Test
    public void test1() throws ExecutionException, InterruptedException {
        AtomicInteger counter = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(1);
        Supplier<Integer> testSupplier = new MemoizedComposedSupplier<>(() -> 0, incrementAndGetWithDelay(counter, latch));
        ExecutorService executorService = PTExecutors.newFixedThreadPool(2);
        List<Future<Integer>> results = new ArrayList<>();
        results.add(executorService.submit(testSupplier::get));
        latch.await();
        results.add(executorService.submit(testSupplier::get));
        for (int i = 0; i < 2; i++) {
            assertThat(results.get(i).get()).isEqualTo(1);
        }
    }

    @Test
    public void test2() throws ExecutionException, InterruptedException {
        AtomicInteger counter = new AtomicInteger(0);
        Supplier<Integer> testSupplier = new MemoizedComposedSupplier<>(() -> counter.incrementAndGet() % 2,
                Function.identity());
        ExecutorService executorService = PTExecutors.newFixedThreadPool(200);
        List<Future<Integer>> results = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            results.add(executorService.submit(testSupplier::get));
        }
        Map<Integer, Long> result = results.stream().map(future -> {
            try {
                System.out.println(future.get());
                return future.get();
            } catch (Exception e) {
                throw new RuntimeException();
            }
        }).collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        System.out.println(result);
    }

    private Function<Integer, Integer> incrementAndGetWithDelay(AtomicInteger counter, CountDownLatch latch) throws InterruptedException {
        return x -> {
            latch.countDown();
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException();
            }
            return counter.incrementAndGet();
        };
    }
}

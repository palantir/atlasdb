/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.timestamp;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Test;

import com.palantir.timestamp.utils.ShufflingTimestampService;

public class ShufflingTimestampServiceTest {
    @Test
    public void shufflingServiceShuffles() throws Exception {
        TimestampService timestampService = new ShufflingTimestampService(new InMemoryTimestampService());
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        CompletionService<Long> completionService = new ExecutorCompletionService<>(executorService);
        List<Future<Long>> timestamps = new ArrayList<>();
        for (int i=0; i<6; i++) {
            timestamps.add(completionService.submit(() -> timestampService.getFreshTimestamp()));
        }

        for (int i=0; i<6; i++) {
            System.out.println(completionService.take().get());
        }
    }
}

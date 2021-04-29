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
package com.palantir.atlasdb.ete;

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.todo.TodoResource;
import java.util.Random;

public final class StreamTestUtils {
    private StreamTestUtils() {
        // utility
    }

    public static void storeFiveStreams(TodoResource todoClient, int streamSize) {
        storeStreams(todoClient, streamSize, 5);
    }

    public static void storeThreeStreams(TodoResource todoClient, int streamSize) {
        storeStreams(todoClient, streamSize, 3);
    }

    private static void storeStreams(TodoResource todoClient, int streamSize, int streamsCount) {
        Random random = new Random();
        byte[] bytes = new byte[streamSize];
        for (int i = 0; i < streamsCount; i++) {
            random.nextBytes(bytes);
            todoClient.storeSnapshot(PtBytes.toString(bytes));
        }
    }
}

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

package com.palantir.atlasdb.ete;

import java.util.Random;

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.todo.TodoResource;

public final class StreamTestUtils {
    private StreamTestUtils() {
        // utility
    }

    public static void storeFiveStreams(TodoResource todoClient, int streamSize) {
        Random random = new Random();
        byte[] bytes = new byte[streamSize];
        for (int i = 0; i < 5; i++) {
            random.nextBytes(bytes);
            todoClient.storeSnapshot(PtBytes.toString(bytes));
        }
    }
}

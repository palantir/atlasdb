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

package com.palantir.atlasdb.sweep.queue;

import java.util.Collection;
import java.util.function.Consumer;

public interface SweepQueueReader {

    /**
     * Passes the next batch of writes to the given consumer. If the consumer returns without throwing,
     * the batch is considered consumed, and future invocations of this method will consume only newer writes.
     * On the other hand, if the consumer fails by throwing an exception, the same batch will be consumed
     * on the next invocation.
     * @param consumer
     * @param maxTimestampExclusive
     */
    void consumeNextBatch(Consumer<Collection<WriteInfo>> consumer, long maxTimestampExclusive);

}

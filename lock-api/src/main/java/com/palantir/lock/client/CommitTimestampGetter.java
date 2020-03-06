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

package com.palantir.lock.client;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import com.palantir.atlasdb.autobatch.Autobatchers;
import com.palantir.atlasdb.autobatch.BatchElement;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsRequest;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsResponse;
import com.palantir.lock.watch.LockWatchEventCache;

public class CommitTimestampGetter implements AutoCloseable {
    private final DisruptorAutobatcher<Void, Long> autobatcher;

    private CommitTimestampGetter(DisruptorAutobatcher<Void, Long> autobatcher) {
        this.autobatcher = autobatcher;
    }

    public static CommitTimestampGetter create(NamespacedConjureTimelockService timelock, LockWatchEventCache cache) {
        DisruptorAutobatcher<Void, Long> autobatcher = Autobatchers
                .independent(consumer(timelock, cache))
                .safeLoggablePurpose("get-commit-timestamp")
                .build();
        return new CommitTimestampGetter(autobatcher);
    }

    public long getCommitTimestamp() {
        return AtlasFutures.getUnchecked(autobatcher.apply(null));
    }

    private static Consumer<List<BatchElement<Void, Long>>> consumer(NamespacedConjureTimelockService timelock,
            LockWatchEventCache cache) {
        return batch -> {
            int count = batch.size();
            List<Long> commitTimestamps = new ArrayList<>();
            while (commitTimestamps.size() < count) {
                GetCommitTimestampsRequest request = GetCommitTimestampsRequest.builder()
                        .numTimestamps(count - commitTimestamps.size())
                        .lastKnownVersion(cache.lastKnownVersion().version())
                        .build();
                GetCommitTimestampsResponse response = timelock.getCommitTimestamps(request);
                commitTimestamps.addAll(process(response, cache));
            }
            for (int i = 0; i < count; i++) {
                batch.get(i).result().set(commitTimestamps.get(i));
            }
        };
    }

    private static List<Long> process(GetCommitTimestampsResponse response, LockWatchEventCache cache) {
        List<Long> timestamps = LongStream
                .rangeClosed(response.getInclusiveLower(), response.getInclusiveUpper())
                .boxed()
                .collect(Collectors.toList());
        cache.processGetCommitTimestampsUpdate(timestamps, response.getLockWatchUpdate());
        return timestamps;
    }

    @Override
    public void close() {
        autobatcher.close();
    }
}

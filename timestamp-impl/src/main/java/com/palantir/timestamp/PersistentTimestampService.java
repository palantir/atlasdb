/**
 * Copyright 2015 Palantir Technologies
 * <p>
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://opensource.org/licenses/BSD-3-Clause
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.timestamp;

import java.util.concurrent.ExecutorService;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Preconditions;
import com.palantir.common.concurrent.PTExecutors;

@ThreadSafe
public class PersistentTimestampService implements TimestampService {
    private static final int MAX_REQUEST_RANGE_SIZE = 10 * 1000;

    private final ExecutorService executor;
    private final AvailableTimestamps availableTimestamps;

    public PersistentTimestampService(AvailableTimestamps availableTimestamps) {
        this.availableTimestamps = availableTimestamps;
        executor = PTExecutors.newSingleThreadExecutor(PTExecutors.newThreadFactory("Timestamp allocator", Thread.NORM_PRIORITY, true));
    }

    public static PersistentTimestampService create(TimestampBoundStore tbs) {
        PersistentUpperLimit upperLimit = new PersistentUpperLimit(tbs);
        LastReturnedTimestamp lastReturned = new LastReturnedTimestamp(upperLimit.get());
        AvailableTimestamps availableTimestamps = new AvailableTimestamps(lastReturned, upperLimit);

        return new PersistentTimestampService(availableTimestamps);
    }

    @Override
    public long getFreshTimestamp() {
        return getFreshTimestamps(1).getLowerBound();
    }

    @Override
    public synchronized TimestampRange getFreshTimestamps(int numTimestampsRequested) {
        int numTimestampsToHandOut = cleanUpTimestampRequest(numTimestampsRequested);
        TimestampRange handedOut = availableTimestamps.handOutTimestamps(numTimestampsToHandOut);
        asynchronouslyRefreshBuffer();
        return handedOut;
    }

    /**
     * Fast forwards the timestamp to the specified one so that no one can be served fresh timestamps prior
     * to it from now on.
     *
     * Sets the upper limit in the TimestampBoundStore as well as increases the minimum timestamp that can
     * be allocated from this instantiation of the TimestampService moving forward.
     *
     * The caller of this is responsible for not using any of the fresh timestamps previously served to it,
     * and must call getFreshTimestamps() to ensure it is using timestamps after the fastforward point.
     *
     * @param newMinimumTimestamp
     */
    public void fastForwardTimestamp(long newMinimumTimestamp) {
        availableTimestamps.fastForwardTo(newMinimumTimestamp);
    }

    private int cleanUpTimestampRequest(int numTimestampsRequested) {
        Preconditions.checkArgument(numTimestampsRequested > 0,
                "Number of timestamps requested must be greater than zero, was %s",
                numTimestampsRequested);

        return Math.min(numTimestampsRequested, MAX_REQUEST_RANGE_SIZE);
    }

    private void asynchronouslyRefreshBuffer() {
        executor.submit(new Runnable() {
            @Override
            public void run() {
                availableTimestamps.refreshBuffer();
            }
        });
    }

}

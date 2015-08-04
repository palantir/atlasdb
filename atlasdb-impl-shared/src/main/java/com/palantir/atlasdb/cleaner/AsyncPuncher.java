/**
 * // Copyright 2015 Palantir Technologies
 * //
 * // Licensed under the BSD-3 License (the "License");
 * // you may not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * // http://opensource.org/licenses/BSD-3-Clause
 * //
 * // Unless required by applicable law or agreed to in writing, software
 * // distributed under the License is distributed on an "AS IS" BASIS,
 * // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * // See the License for the specific language governing permissions and
 * // limitations under the License.
 */
package com.palantir.atlasdb.cleaner;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Supplier;
import com.palantir.common.concurrent.PTExecutors;

/**
 * Wrap another Puncher, optimizing the #punch() operation to operate just on a local variable; the
 * underlying "real" punch operation is only invoked asynchronously at a fixed interval, with the
 * latest supplied timestamp as the parameter.
 *
 * @author jweel
 */
public class AsyncPuncher implements Puncher {
    public static AsyncPuncher create(Puncher delegate, long interval) {
        AsyncPuncher asyncPuncher = new AsyncPuncher(delegate, interval);
        asyncPuncher.start();
        return asyncPuncher;
    }

    private final ScheduledExecutorService service = PTExecutors.newSingleThreadScheduledExecutor();

    private final Puncher delegate;
    private final long interval;

    private AsyncPuncher(Puncher delegate, long interval) {
        this.delegate = delegate;
        this.interval = interval;
    }

    private volatile Long lastTimestamp = null;

    private void start() {
        service.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                if (lastTimestamp != null) {
                    delegate.punch(lastTimestamp);
                    lastTimestamp = null;
                }
            }
        }, 0, interval, TimeUnit.MILLISECONDS);
    }

    @Override
    public void punch(long timestamp) {
        lastTimestamp = timestamp;
    }

    @Override
    public Supplier<Long> getTimestampSupplier() {
        return delegate.getTimestampSupplier();
    }

    @Override
    public void shutdown() {
        delegate.shutdown();
        service.shutdown();
    }
}

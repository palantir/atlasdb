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

package com.palantir.atlasdb.limiter;

import com.google.common.util.concurrent.RateLimiter;
import com.google.errorprone.annotations.MustBeClosed;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.refreshable.Refreshable;
import java.io.Closeable;
import javax.ws.rs.ServiceUnavailableException;
import org.immutables.value.Value;

public class ConfiguredClientLimiter implements AtlasClientLimiter {

    private final Refreshable<Config> config;
    private final ResizableSemaphore rangeScanConcurrency;
    private final RateLimiter rowReadRateLimiter;

    public ConfiguredClientLimiter(Refreshable<Config> config) {
        this.config = config;

        this.rangeScanConcurrency = new ResizableSemaphore(0);
        this.rowReadRateLimiter = RateLimiter.create(config.get().rowsReadPerSecondLimit());
    }

    @Override
    @MustBeClosed
    public Closeable limitRangeScan(TableReference _tableRef) {
        rangeScanConcurrency.resize(config.get().concurrentRangeScans());

        if (rangeScanConcurrency.tryAcquire()) {
            return rangeScanConcurrency::release;
        }

        throw new ServiceUnavailableException();
    }

    @Override
    public void limitRowsRead(TableReference _tableRef, int rows) {
        if (rowReadRateLimiter.tryAcquire(rows)) {
            return;
        }

        throw new ServiceUnavailableException();
    }

    @Value.Immutable
    public interface Config {
        int concurrentRangeScans();

        double rowsReadPerSecondLimit();

        class Builder extends ImmutableConfig.Builder {}

        static ImmutableConfig.Builder builder() {
            return new Builder();
        }
    }
}

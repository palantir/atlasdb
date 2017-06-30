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

package com.palantir.atlasdb.factory.timestamp;

import java.util.function.Supplier;

import javax.ws.rs.QueryParam;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.config.TimestampClientConfig;
import com.palantir.atlasdb.util.JavaSuppliers;
import com.palantir.timestamp.RequestBatchingTimestampService;
import com.palantir.timestamp.TimestampRange;
import com.palantir.timestamp.TimestampService;

public class DynamicDecoratedTimestampService implements TimestampService {
    private final TimestampService decoratedService;
    private final TimestampService delegateService;
    private final Supplier<Boolean> shouldDecorate;

    @VisibleForTesting
    DynamicDecoratedTimestampService(
            TimestampService decoratedService,
            TimestampService delegateService,
            Supplier<Boolean> shouldDecorate) {
        this.decoratedService = decoratedService;
        this.delegateService = delegateService;
        this.shouldDecorate = shouldDecorate;
    }

    public static DynamicDecoratedTimestampService createWithRateLimiting(
            TimestampService delegateService, Supplier<TimestampClientConfig> configSupplier) {
        return new DynamicDecoratedTimestampService(
                new RequestBatchingTimestampService(delegateService),
                delegateService,
                JavaSuppliers.compose(TimestampClientConfig::enableTimestampBatching, configSupplier));
    }

    @Override
    public long getFreshTimestamp() {
        if (shouldDecorate.get()) {
            return decoratedService.getFreshTimestamp();
        }
        return delegateService.getFreshTimestamp();
    }

    @Override
    public TimestampRange getFreshTimestamps(@QueryParam("number") int numTimestampsRequested) {
        if (shouldDecorate.get()) {
            return decoratedService.getFreshTimestamps(numTimestampsRequested);
        }
        return delegateService.getFreshTimestamps(numTimestampsRequested);
    }
}

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

import com.palantir.atlasdb.config.TimestampClientConfig;
import com.palantir.atlasdb.factory.DynamicDecoratingProxy;
import com.palantir.atlasdb.factory.ServiceCreator;
import com.palantir.atlasdb.transaction.impl.TimelockTimestampServiceAdapter;
import com.palantir.atlasdb.transaction.impl.TimestampDecoratingTimelockService;
import com.palantir.atlasdb.util.JavaSuppliers;
import com.palantir.lock.v2.TimelockService;
import com.palantir.timestamp.RequestBatchingTimestampService;
import com.palantir.timestamp.TimestampService;

public final class DecoratedTimelockServices {
    private DecoratedTimelockServices() {
        // factory
    }

    public static TimelockService createTimelockServiceWithTimestampBatching(
            TimelockService timelockService,
            Supplier<TimestampClientConfig> configSupplier) {
        return DynamicDecoratingProxy.newProxyInstance(
                new TimestampDecoratingTimelockService(timelockService,
                        createRequestBatchingTimestampService(timelockService)),
                timelockService,
                JavaSuppliers.compose(TimestampClientConfig::enableTimestampBatching, configSupplier),
                TimelockService.class);
    }

    private static TimestampService createRequestBatchingTimestampService(
            TimelockService timelockService) {
        return ServiceCreator.createInstrumentedService(
                new RequestBatchingTimestampService(new TimelockTimestampServiceAdapter(timelockService)),
                TimestampService.class);
    }
}

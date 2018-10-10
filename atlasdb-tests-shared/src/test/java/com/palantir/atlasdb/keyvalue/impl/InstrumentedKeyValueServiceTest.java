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
package com.palantir.atlasdb.keyvalue.impl;

import com.codahale.metrics.MetricRegistry;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.util.AtlasDbMetrics;

public class InstrumentedKeyValueServiceTest extends AbstractKeyValueServiceTest {

    private static final String METRIC_PREFIX = "test.instrumented." + KeyValueService.class.getName();

    @Override
    protected KeyValueService getKeyValueService() {
        return AtlasDbMetrics.instrument(
                new MetricRegistry(),
                KeyValueService.class,
                new InMemoryKeyValueService(false),
                METRIC_PREFIX);
    }

    /**
     * Tear down KVS so that it is recreated and captures metrics for each test.
     */
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        tearDownKvs();
    }

}
